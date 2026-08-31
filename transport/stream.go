package transport

import (
	"errors"
	"fmt"
	"time"

	sipgo "github.com/emiago/sipgo/sip"
)

var (
	// errDesyncSize means a single incomplete message grew past MaxPartialMessageSize.
	errDesyncSize = errors.New("sip stream desynchronized: parser buffer over limit")
	// errDesyncAge means a single incomplete message stayed incomplete past MaxPartialMessageAge.
	errDesyncAge = errors.New("sip stream desynchronized: message incomplete too long")
)

// streamParser wraps a SIP stream parser and reports when the stream can no
// longer be framed.
//
// A message that stops mid-write leaves the parser holding a fragment it can
// never complete. Later bytes on the same connection are appended behind that
// fragment and parsed as a continuation of it, so no further message is ever
// framed. The socket keeps draining at the kernel level, which makes this
// indistinguishable from a healthy idle connection: reads keep succeeding, so
// neither an idle timeout nor a response timeout notices.
//
// A partial message is normal on a stream transport, so the only way to tell
// the two apart is to bound how large and how old an incomplete message may
// get. Both bounds are needed. A busy connection trips the size bound quickly,
// while a quiet one can sit under it indefinitely.
type streamParser struct {
	par *sipgo.ParserStream
	// partialSince is when the current incomplete message was first seen.
	// Zero when no message is pending.
	partialSince time.Time
	// pendingBytes counts bytes fed since the last message was framed. The
	// parser's own buffer is not usable for this: it consumes header lines as
	// it parses them, so it stays near empty however large the incomplete
	// message grows.
	pendingBytes int
	// now is overridable in tests.
	now func() time.Time
}

func newStreamParser(par *sipgo.ParserStream) *streamParser {
	return &streamParser{par: par, now: time.Now}
}

// parse feeds data to the parser and calls onMsg for every complete message.
// A returned error means the connection must be closed.
func (s *streamParser) parse(data []byte, onMsg func(msg sipgo.Message)) error {
	s.pendingBytes += len(data)

	parsed := 0
	err := s.par.ParseSIPStream(data, func(msg sipgo.Message) {
		parsed++
		onMsg(msg)
	})

	// Framing a message is the only thing that counts as progress, and it is
	// what resets both budgets. A read often completes a message and leaves the
	// start of the next one behind, so this has to run before the check below,
	// otherwise a busy connection looks stuck.
	if parsed > 0 {
		s.partialSince = time.Time{}
		s.pendingBytes = 0
	}

	if errors.Is(err, sipgo.ErrParseSipPartial) {
		return s.checkPartial()
	}
	// Any other error is returned so the caller closes the connection. There is
	// no reliable way to find the next message boundary in a byte stream, so
	// reconnecting is the only way back to a known one.
	return err
}

// checkPartial decides whether a pending incomplete message is still plausible.
func (s *streamParser) checkPartial() error {
	if s.pendingBytes > MaxPartialMessageSize {
		return fmt.Errorf("%w: %d bytes without a complete message", errDesyncSize, s.pendingBytes)
	}

	now := s.now()
	if s.partialSince.IsZero() {
		s.partialSince = now
		return nil
	}
	if age := now.Sub(s.partialSince); age > MaxPartialMessageAge {
		return fmt.Errorf("%w: incomplete for %s", errDesyncAge, age)
	}
	return nil
}

// closeReason maps a parse failure to a metric label.
func closeReason(err error) string {
	switch {
	case errors.Is(err, errDesyncSize):
		return "desync_size"
	case errors.Is(err, errDesyncAge):
		return "desync_age"
	default:
		return "parse_error"
	}
}
