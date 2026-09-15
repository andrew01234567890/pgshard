package grpccreds

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// tlsRecordHandshake is the first byte of every TLS connection: the record
// type of the ClientHello. A plaintext gRPC client opens with the HTTP/2
// connection preface, "PRI * HTTP/2.0", and gRPC has no upgrade path that
// would send anything else first.
const tlsRecordHandshake = 0x16

// eitherListener serves a connection with TLS when it opens with a TLS
// record and in plaintext otherwise. See AcceptPlaintext.
type eitherListener struct {
	tls credentials.TransportCredentials
}

func (e eitherListener) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	// grpc sets the connection deadline before the handshake, so a client
	// that connects and sends nothing is cut off here as it would be in a
	// TLS handshake.
	first := make([]byte, 1)
	if _, err := io.ReadFull(raw, first); err != nil {
		return nil, nil, err
	}
	conn := &replayConn{Conn: raw, pending: first}
	if first[0] == tlsRecordHandshake {
		return e.tls.ServerHandshake(conn)
	}
	return insecure.NewCredentials().ServerHandshake(conn)
}

func (e eitherListener) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("grpccreds: listener credentials cannot dial")
}

func (e eitherListener) Info() credentials.ProtocolInfo { return e.tls.Info() }

func (e eitherListener) Clone() credentials.TransportCredentials {
	return eitherListener{tls: e.tls.Clone()}
}

func (e eitherListener) OverrideServerName(name string) error {
	//nolint:staticcheck // part of the interface grpc still defines
	return e.tls.OverrideServerName(name)
}

// replayConn hands back the bytes already read from the connection before
// reading more.
type replayConn struct {
	net.Conn
	pending []byte
}

func (c *replayConn) Read(b []byte) (int, error) {
	if len(c.pending) > 0 {
		n := copy(b, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	return c.Conn.Read(b)
}

// SyscallConn keeps the socket reachable for what grpc reads off it, as the
// TLS credentials' own wrapper does.
func (c *replayConn) SyscallConn() (syscall.RawConn, error) {
	sc, ok := c.Conn.(syscall.Conn)
	if !ok {
		return nil, errors.New("grpccreds: connection does not expose its socket")
	}
	return sc.SyscallConn()
}
