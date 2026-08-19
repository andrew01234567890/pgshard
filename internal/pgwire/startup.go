package pgwire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Startup packet request codes (see PostgreSQL src/include/libpq/pqcomm.h).
const (
	ProtocolVersion30 = 196608 // 3.0
	ProtocolVersion32 = 196610 // 3.2
	// ProtocolVersionLatest is the newest minor version this server speaks.
	ProtocolVersionLatest = ProtocolVersion32

	sslRequestCode    = 80877103
	gssEncRequestCode = 80877104
	cancelRequestCode = 80877102

	minStartupPacketLen = 4
	maxStartupPacketLen = 10000
	maxCancelKeyLen     = 256
)

type startupKind int

const (
	startupSSLRequest startupKind = iota + 1
	startupGSSEncRequest
	startupCancelRequest
	startupMessage
)

// startupPacket is a decoded startup-phase packet. Unlike pgproto3 it keeps
// unknown minor versions so the session can negotiate them down.
type startupPacket struct {
	kind            startupKind
	protocolVersion uint32
	params          map[string]string
	// cancelPID and cancelKey are set for cancel requests.
	cancelPID uint32
	cancelKey []byte
}

func (p *startupPacket) major() uint32 { return p.protocolVersion >> 16 }
func (p *startupPacket) minor() uint32 { return p.protocolVersion & 0xffff }

func readStartupPacket(r *bufio.Reader) (*startupPacket, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := int(int32(binary.BigEndian.Uint32(hdr[:]))) - 4
	if n < minStartupPacketLen || n > maxStartupPacketLen {
		return nil, fmt.Errorf("invalid length of startup packet: %d", n+4)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return decodeStartupPacket(body)
}

func decodeStartupPacket(body []byte) (*startupPacket, error) {
	if len(body) < 4 {
		return nil, errors.New("startup packet too short")
	}
	code := binary.BigEndian.Uint32(body)
	rest := body[4:]
	switch code {
	case sslRequestCode, gssEncRequestCode:
		if len(rest) != 0 {
			return nil, errors.New("startup packet: trailing bytes after request code")
		}
		if code == sslRequestCode {
			return &startupPacket{kind: startupSSLRequest}, nil
		}
		return &startupPacket{kind: startupGSSEncRequest}, nil
	case cancelRequestCode:
		if len(rest) < 8 || len(rest)-4 > maxCancelKeyLen {
			return nil, fmt.Errorf("cancel request: invalid key length %d", len(rest)-4)
		}
		return &startupPacket{
			kind:      startupCancelRequest,
			cancelPID: binary.BigEndian.Uint32(rest),
			cancelKey: append([]byte(nil), rest[4:]...),
		}, nil
	}
	p := &startupPacket{kind: startupMessage, protocolVersion: code, params: map[string]string{}}
	if p.major() != 3 {
		// The caller reports the version to the client; params are meaningless.
		return p, nil
	}
	for {
		if len(rest) == 0 {
			return nil, errors.New("startup packet: missing terminator")
		}
		if rest[0] == 0 {
			if len(rest) != 1 {
				return nil, errors.New("startup packet: bytes after terminator")
			}
			return p, nil
		}
		k := bytes.IndexByte(rest, 0)
		if k < 0 {
			return nil, errors.New("startup packet: unterminated key")
		}
		key := string(rest[:k])
		rest = rest[k+1:]
		v := bytes.IndexByte(rest, 0)
		if v < 0 {
			return nil, errors.New("startup packet: unterminated value")
		}
		p.params[key] = string(rest[:v])
		rest = rest[v+1:]
	}
}
