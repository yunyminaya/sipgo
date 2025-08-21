package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"

	sipgo "github.com/emiago/sipgo/sip"

	"github.com/livekit/sipgo/sip"
)

// TLS transport implementation
type TLSTransport struct {
	*TCPTransport

	// rootPool *x509.CertPool
	tlsConf *tls.Config
}

// NewTLSTransport needs dialTLSConf for creating connections when dialing
func NewTLSTransport(log *slog.Logger, par *sipgo.Parser, dialTLSConf *tls.Config) *TLSTransport {
	tcptrans := NewTCPTransport(log, par)
	tcptrans.transport = TransportTLS //Override transport
	p := &TLSTransport{
		TCPTransport: tcptrans,
	}

	// p.rootPool = roots
	p.tlsConf = dialTLSConf
	//p.log = log.With("caller", "transport<TLS>")
	return p
}

func (t *TLSTransport) String() string {
	return "transport<TLS>"
}

// CreateConnection creates TLS connection for TCP transport
func (t *TLSTransport) CreateConnection(laddr Addr, host string, raddr Addr, handler sip.MessageHandler) (Connection, error) {
	// raddr, err := net.ResolveTCPAddr("tcp", addr)
	// if err != nil {
	// 	return nil, err
	// }

	traddr := &net.TCPAddr{
		IP:   raddr.IP,
		Port: raddr.Port,
	}

	return t.createConnection(nil, host, traddr, handler)
}

func (t *TLSTransport) createConnection(laddr *net.TCPAddr, host string, raddr *net.TCPAddr, handler sip.MessageHandler) (Connection, error) {
	addr := raddr.String()
	log := t.log.With("raddr", addr, "host", host)
	log.Info("Dialing new TLS connection")
	fmt.Printf("Dialing new TLS connection: host=%s, addr=%s, log=%T(%v)\n", host, addr, log.Handler(), log.Handler())
	debug.PrintStack()

	//TODO does this need to be each config
	// SHould we make copy of rootPool?
	// There is Clone of config

	conf := t.tlsConf.Clone()
	if conf == nil {
		conf = &tls.Config{}
	}
	conf.ServerName = host
	dialer := &net.Dialer{
		LocalAddr: laddr,
	}
	conn, err := dialer.DialContext(context.TODO(), "tcp", addr)
	if err != nil {
		log.Warn("Failed to dial new TLS connection", "err", err)
		return nil, fmt.Errorf("%s dial err=%w", t, err)
	}
	tconn := tls.Client(conn, conf)

	c := t.initConnection(log, tconn, addr, handler)
	return c, nil
}
