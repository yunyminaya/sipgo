package transport

import (
	"log/slog"
	"sync"
)

// TODO Connection pool with keeping active connections longer

// ConnectionPool holds the connections for one transport, keyed by remote
// address. An address can map to several connections.
type ConnectionPool struct {
	// TODO consider sync.Map way with atomic checks to reduce mutex contention
	sync.RWMutex
	m map[string][]Connection
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		m: make(map[string][]Connection),
	}
}

func (p *ConnectionPool) Add(a string, c Connection) {
	if c.Ref(0) < 1 {
		c.Ref(1) // Make 1 reference count by default
	}
	p.Lock()
	p.m[a] = append(p.m[a], c)
	p.Unlock()
}

// Get returns the most recently added connection for an address, or nil if
// there is none.
//
// Getting connection pool increases reference.
// Make sure you TryClose after finish
func (p *ConnectionPool) Get(a string) (c Connection) {
	p.RLock()
	conns := p.m[a]
	if len(conns) > 0 {
		c = conns[len(conns)-1]
	}
	p.RUnlock()
	if c == nil {
		return nil
	}
	c.Ref(1)
	// TODO handling more references
	// if c.Ref(1) <= 1 {
	// 	return nil
	// }

	return c
}

// CloseAndDelete closes a connection and removes that connection from the
// pool. Other connections to the same address are left alone.
func (p *ConnectionPool) CloseAndDelete(c Connection, addr string) {
	p.Lock()
	defer p.Unlock()
	ref, _ := c.TryClose() // Be nice. Saves from double closing
	if ref > 0 {
		if err := c.Close(); err != nil {
			slog.Warn("Closing conection return error", "err", err)
		}
	}
	p.removeLocked(c, addr)
}

// removeLocked drops one connection from an address. Caller holds the lock.
func (p *ConnectionPool) removeLocked(c Connection, addr string) {
	conns := p.m[addr]
	for i, existing := range conns {
		if existing != c {
			continue
		}
		conns = append(conns[:i], conns[i+1:]...)
		if len(conns) == 0 {
			delete(p.m, addr)
		} else {
			p.m[addr] = conns
		}
		return
	}
}

// Clear will clear all connection from pool and close them
func (p *ConnectionPool) Clear() {
	p.Lock()
	defer p.Unlock()
	for _, conns := range p.m {
		for _, c := range conns {
			if c.Ref(0) <= 0 {
				continue
			}
			if err := c.Close(); err != nil {
				slog.Warn("Closing conection return error", "err", err)
			}
		}
	}
	// Remove all
	p.m = make(map[string][]Connection)
}

// Size returns the total number of pooled connections across all addresses.
func (p *ConnectionPool) Size() int {
	p.RLock()
	l := 0
	for _, conns := range p.m {
		l += len(conns)
	}
	p.RUnlock()
	return l
}
