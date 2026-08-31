package transport

import (
	"log/slog"
	"net"
	"os"
	"testing"

	"github.com/livekit/sipgo/fakes"
)

func TestConnectionPool(t *testing.T) {
	pool := NewConnectionPool()

	fakeConn := &fakes.TCPConn{
		LAddr:  net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060},
		RAddr:  net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 5060},
		Reader: nil,
		Writer: nil,
	}
	conn := &TCPConnection{Conn: fakeConn}

	pool.Add(fakeConn.RAddr.String(), conn)

	c := pool.Get(fakeConn.RAddr.String())
	if c != conn {
		t.Fatal("Not found connection")
	}
}

func BenchmarkConnectionPool(b *testing.B) {
	slog.SetDefault(slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}),
	))
	pool := NewConnectionPool()

	for i := 0; i < b.N; i++ {
		conn := &TCPConnection{Conn: &fakes.TCPConn{
			LAddr:  net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060},
			RAddr:  net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 5060},
			Reader: nil,
			Writer: nil,
		}}
		a := &net.TCPAddr{
			IP:   net.IPv4('1', '2', '3', byte(i)),
			Port: 1000,
		}
		pool.Add(a.String(), conn)
		c := pool.Get(a.String())
		if c != conn {
			b.Fatal("mismatched function")
		}
	}
}

// Several connections can share a remote address, and closing one must not
// evict the others.
func TestConnectionPoolMultiplePerAddress(t *testing.T) {
	pool := NewConnectionPool()
	addr := "127.0.0.2:5060"

	newConn := func() *TCPConnection {
		return &TCPConnection{Conn: &fakes.TCPConn{
			LAddr: net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060},
			RAddr: net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 5060},
		}}
	}
	first, second := newConn(), newConn()

	pool.Add(addr, first)
	pool.Add(addr, second)
	if got := pool.Size(); got != 2 {
		t.Fatalf("expected 2 pooled connections, got %d", got)
	}

	// Get returns the most recent one.
	if c := pool.Get(addr); c != second {
		t.Fatal("expected the most recently added connection")
	}

	// Closing the older connection leaves the newer one reachable. Previously
	// this deleted the address outright and took the newer one with it.
	pool.CloseAndDelete(first, addr)
	if got := pool.Size(); got != 1 {
		t.Fatalf("expected 1 pooled connection, got %d", got)
	}
	if c := pool.Get(addr); c != second {
		t.Fatal("closing the older connection evicted the newer one")
	}

	pool.CloseAndDelete(second, addr)
	if got := pool.Size(); got != 0 {
		t.Fatalf("expected empty pool, got %d", got)
	}
	if c := pool.Get(addr); c != nil {
		t.Fatal("expected nil for an unknown address")
	}
}
