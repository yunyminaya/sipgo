package transport

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sipgo "github.com/emiago/sipgo/sip"
)

// invite builds a well formed INVITE with a body of the given size.
func invite(callID string, bodySize int) string {
	body := strings.Repeat("x", bodySize)
	return "INVITE sip:bob@example.com SIP/2.0\r\n" +
		"Via: SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bK." + callID + "\r\n" +
		"From: <sip:alice@example.com>;tag=" + callID + "\r\n" +
		"To: <sip:bob@example.com>\r\n" +
		"Call-ID: " + callID + "\r\n" +
		"CSeq: 1 INVITE\r\n" +
		fmt.Sprintf("Content-Length: %d\r\n", len(body)) +
		"\r\n" + body
}

func newTestParser() *streamParser {
	return newStreamParser(sipgo.NewParser().NewSIPStream())
}

// collect returns a callback that records parsed messages.
func collect(msgs *[]sipgo.Message) func(sipgo.Message) {
	return func(m sipgo.Message) { *msgs = append(*msgs, m) }
}

func TestStreamParserWholeMessage(t *testing.T) {
	p := newTestParser()
	var msgs []sipgo.Message

	if err := p.parse([]byte(invite("a", 10)), collect(&msgs)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if !p.partialSince.IsZero() {
		t.Fatal("expected no pending partial message")
	}
}

// A message split across many small reads must still parse. This is the
// false-positive trap: partial data is normal on a stream transport.
func TestStreamParserSplitAcrossReads(t *testing.T) {
	p := newTestParser()
	var msgs []sipgo.Message

	data := []byte(invite("split", 400))
	for i := 0; i < len(data); i++ {
		if err := p.parse(data[i:i+1], collect(&msgs)); err != nil {
			t.Fatalf("byte %d: unexpected error: %v", i, err)
		}
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
}

// Two whole messages in one read must both be delivered.
func TestStreamParserPipelined(t *testing.T) {
	p := newTestParser()
	var msgs []sipgo.Message

	both := invite("one", 10) + invite("two", 10)
	if err := p.parse([]byte(both), collect(&msgs)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
}

// This pins behaviour of the upstream parser, not of streamParser, because the
// bounds above are sized around it. A message that stops mid write does not
// stop the parser from framing later messages: the fragment merges with
// whatever arrives next, one corrupted message comes out carrying headers from
// both, and the stream then realigns on its own.
//
// That is why the age bound matters more than the size bound here. Nothing
// accumulates, so size only catches a fragment that nothing ever follows, and
// containing this case means limiting how many calls share a connection.
//
// If a parser upgrade changes this, the reasoning behind those bounds no longer
// holds and this test should fail rather than the change going unnoticed.
func TestUpstreamParserTruncationMergesThenRealigns(t *testing.T) {
	p := newTestParser()
	var msgs []sipgo.Message

	// A message that stops inside a header value, before its Content-Length.
	truncated := "INVITE sip:bob@example.com SIP/2.0\r\n" +
		"Via: SIP/2.0/TCP 10.0.0.1:5060;branch=z9hG4bK.trunc\r\n" +
		"Call-ID: TRUNCATED\r\n" +
		"CSeq: 1 INVITE\r\n" +
		"Min-SE:"
	if err := p.parse([]byte(truncated), collect(&msgs)); err != nil {
		t.Fatalf("a truncated message alone should look partial, got: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected no messages yet, got %d", len(msgs))
	}

	// The next message completes the fragment instead of being parsed on its
	// own, so it is consumed and its content is lost.
	if err := p.parse([]byte(invite("VICTIM", 40)), collect(&msgs)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 merged message, got %d", len(msgs))
	}
	// The merged message has a mixed identity: the routing of the message that
	// was cut off, and the Call-ID of the one that completed it. A response
	// shaped like this cannot be matched to the transaction that is waiting for
	// it, which is how a call ends up timing out with no final response.
	merged := msgs[0]
	via := merged.Via()
	if via == nil {
		t.Fatal("merged message has no Via")
	}
	if !strings.Contains(via.Value(), "z9hG4bK.trunc") {
		t.Fatalf("expected the truncated message's branch in Via, got %q", via.Value())
	}
	if got := merged.CallID().Value(); got != "VICTIM" {
		t.Fatalf("expected the completing message's Call-ID, got %q", got)
	}

	// From here the stream is aligned again and later messages are intact.
	msgs = msgs[:0]
	for i := 0; i < 3; i++ {
		if err := p.parse([]byte(invite(fmt.Sprintf("AFTER%d", i), 40)), collect(&msgs)); err != nil {
			t.Fatalf("round %d: unexpected error: %v", i, err)
		}
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 clean messages after realignment, got %d", len(msgs))
	}
	for i, m := range msgs {
		want := fmt.Sprintf("AFTER%d", i)
		if got := m.CallID().Value(); got != want {
			t.Fatalf("message %d: expected Call-ID %q, got %q", i, want, got)
		}
	}
}

// A fragment that nothing ever completes is what the size bound catches: the
// buffer grows without a message ever being framed.
func TestStreamParserDesyncOnSize(t *testing.T) {
	p := newTestParser()
	var msgs []sipgo.Message

	if err := p.parse([]byte("INVITE sip:bob@example.com SIP/2.0\r\n"), collect(&msgs)); err != nil {
		t.Fatalf("unexpected error on start line: %v", err)
	}

	// Header lines that never terminate the message.
	filler := "X-Pad: " + strings.Repeat("y", 900) + "\r\n"
	var err error
	for sent := 0; sent <= MaxPartialMessageSize+len(filler); sent += len(filler) {
		if err = p.parse([]byte(filler), collect(&msgs)); err != nil {
			break
		}
	}
	if !errors.Is(err, errDesyncSize) {
		t.Fatalf("expected errDesyncSize, got: %v", err)
	}
	if got := closeReason(err); got != "desync_size" {
		t.Fatalf("expected reason desync_size, got %q", got)
	}
}

// A quiet connection can stay under the size bound indefinitely, so the age
// bound has to catch it.
func TestStreamParserDesyncOnAge(t *testing.T) {
	p := newTestParser()
	now := time.Now()
	p.now = func() time.Time { return now }
	var msgs []sipgo.Message

	if err := p.parse([]byte("INVITE sip:bob@example.com SIP/2.0\r\nCall-ID: slow\r\n"), collect(&msgs)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.partialSince.IsZero() {
		t.Fatal("expected a pending partial message")
	}

	// Still inside the window.
	now = now.Add(MaxPartialMessageAge / 2)
	if err := p.parse([]byte("Via: SIP/2.0/TCP 10.0.0.1:5060\r\n"), collect(&msgs)); err != nil {
		t.Fatalf("unexpected error inside window: %v", err)
	}

	// Past the window.
	now = now.Add(MaxPartialMessageAge + time.Second)
	err := p.parse([]byte("From: <sip:alice@example.com>\r\n"), collect(&msgs))
	if !errors.Is(err, errDesyncAge) {
		t.Fatalf("expected errDesyncAge, got: %v", err)
	}
	if got := closeReason(err); got != "desync_age" {
		t.Fatalf("expected reason desync_age, got %q", got)
	}
}

// Progress must reset the age bound, otherwise a busy connection is killed
// while it is working correctly.
func TestStreamParserProgressResetsAge(t *testing.T) {
	p := newTestParser()
	now := time.Now()
	p.now = func() time.Time { return now }
	var msgs []sipgo.Message

	for i := 0; i < 10; i++ {
		// Leave a fragment behind on every read, then complete it on the next.
		whole := invite(fmt.Sprintf("m%d", i), 100)
		split := len(whole) - 20
		if err := p.parse([]byte(whole[:split]), collect(&msgs)); err != nil {
			t.Fatalf("round %d head: %v", i, err)
		}
		now = now.Add(MaxPartialMessageAge - time.Second)
		if err := p.parse([]byte(whole[split:]), collect(&msgs)); err != nil {
			t.Fatalf("round %d tail: %v", i, err)
		}
		now = now.Add(MaxPartialMessageAge - time.Second)
	}
	if len(msgs) != 10 {
		t.Fatalf("expected 10 messages, got %d", len(msgs))
	}
}

// A hard parse error must be reported so the caller closes the connection,
// rather than being logged and swallowed.
func TestStreamParserHardErrorReported(t *testing.T) {
	p := newTestParser()
	var msgs []sipgo.Message

	// A CR that is not followed by LF. The upstream parser reports this and
	// never advances past the offending byte, so retrying is futile.
	err := p.parse([]byte("INVITE sip:bob@example.com SIP/2.0\rVia: x\r\n\r\n"), collect(&msgs))
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := closeReason(err); got != "parse_error" {
		t.Fatalf("expected reason parse_error, got %q", got)
	}
}

// On a busy connection almost every read both completes a message and leaves
// the start of the next one behind. That has to count as progress, otherwise a
// healthy connection is closed as soon as it has been busy for longer than
// MaxPartialMessageAge.
func TestStreamParserPartialTailAfterProgressIsNotDesync(t *testing.T) {
	p := newTestParser()
	now := time.Now()
	p.now = func() time.Time { return now }
	var msgs []sipgo.Message

	const count = 6
	var stream string
	for i := 0; i < count; i++ {
		stream += invite(fmt.Sprintf("BUSY%d", i), 40)
	}

	// Chunk so that every read ends part way through a message.
	chunk := len(invite("BUSY0", 40)) + 37
	for off := 0; off < len(stream); off += chunk {
		end := min(off+chunk, len(stream))
		if err := p.parse([]byte(stream[off:end]), collect(&msgs)); err != nil {
			t.Fatalf("offset %d: healthy traffic reported as desync: %v", off, err)
		}
		// Each read is well inside the window, but they add up to far more
		// than it across the sequence.
		now = now.Add(MaxPartialMessageAge / 3)
	}

	if len(msgs) != count {
		t.Fatalf("expected %d messages, got %d", count, len(msgs))
	}
	// The stream ends on a message boundary, so the final read frames a
	// message and leaves nothing pending.
	if p.pendingBytes != 0 {
		t.Fatalf("expected pendingBytes cleared after the last message, got %d", p.pendingBytes)
	}
}
