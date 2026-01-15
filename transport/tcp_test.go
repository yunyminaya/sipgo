package transport

import (
	"net"
	"testing"
)

func TestTCPBindRefused(t *testing.T) {
	conn, err := bindRange(10000, 10001, func(lport int) (*net.TCPConn, error) {
		return net.DialTCP("tcp", &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: lport,
		}, &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 9999, // dial would fail
		})
	})
	if err == nil {
		conn.Close()
		t.Fatal("expected error")
	}
	if err == ErrCannotBindPort {
		t.Fatal("unexpected error")
	}
}

func TestTCPBindNoPorts(t *testing.T) {
	l1, err := net.Listen("tcp", "127.0.0.1:10000")
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()
	l2, err := net.Listen("tcp", "127.0.0.1:10001")
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	conn, err := bindRange(10000, 10001, func(lport int) (*net.TCPConn, error) {
		return net.DialTCP("tcp", &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: lport,
		}, &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 9999, // dial would fail
		})
	})
	if err == nil {
		conn.Close()
		t.Fatal("expected error")
	}
	if err != ErrCannotBindPort {
		t.Fatal("unexpected error")
	}
}
