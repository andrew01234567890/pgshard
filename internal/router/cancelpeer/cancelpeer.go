// Package cancelpeer forwards client cancel requests between router
// instances: a CancelRequest may reach any router pod behind a Service, while
// the session it names lives on the pod that minted the key.
package cancelpeer

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// InstanceOf returns the router instance id embedded in a protocol 3.2
// cancel key. Protocol 3.0 keys carry no prefix and report ok=false.
func InstanceOf(key pgwire.CancelKey) (id uint32, ok bool) {
	if len(key.Secret) != pgwire.CancelKeyLen32 {
		return 0, false
	}
	return binary.BigEndian.Uint32(key.Secret[0:4]), true
}

// Config wires a Forwarder.
type Config struct {
	// Self is the local instance id; keys minted here are never forwarded.
	Self uint32
	// Static maps peer instance ids to their RouterPeer addresses.
	Static map[uint32]string
	// Service is a host:port whose A/AAAA records enumerate peers with
	// unknown ids (a headless Kubernetes Service). Empty disables discovery.
	Service string
	Creds   credentials.TransportCredentials
	// Rate and Burst bound forwarded cancels per second; zero picks 50/50.
	Rate  float64
	Burst int
	// Timeout bounds one peer RPC; zero picks 2s.
	Timeout time.Duration
	Logger  *slog.Logger
	// Resolve overrides DNS lookup (tests).
	Resolve func(ctx context.Context, host string) ([]string, error)
	// Dial overrides the gRPC client factory (tests).
	Dial func(addr string) (pgshardv1.RouterPeerClient, error)
}

// Forwarder decides which peers a cancel key concerns and delivers it.
type Forwarder struct {
	cfg     Config
	limiter *bucket

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	// dropped counts cancels refused by the rate limit.
	dropped int
}

// New validates cfg and returns a Forwarder.
func New(cfg Config) (*Forwarder, error) {
	if cfg.Rate <= 0 {
		cfg.Rate = 50
	}
	if cfg.Burst <= 0 {
		cfg.Burst = 50
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Resolve == nil {
		cfg.Resolve = func(ctx context.Context, host string) ([]string, error) {
			return net.DefaultResolver.LookupHost(ctx, host)
		}
	}
	if cfg.Dial == nil && cfg.Creds == nil && (len(cfg.Static) > 0 || cfg.Service != "") {
		return nil, fmt.Errorf("cancelpeer: transport credentials are required")
	}
	f := &Forwarder{cfg: cfg, limiter: newBucket(cfg.Rate, cfg.Burst), conns: map[string]*grpc.ClientConn{}}
	if f.cfg.Dial == nil {
		f.cfg.Dial = f.dial
	}
	return f, nil
}

// Targets returns the peer addresses a key must be forwarded to, sorted.
// A 3.2 key goes to the instance it names (or, when that id is not
// statically known, to every discovered peer); a 3.0 key, which names no
// instance, goes to every peer. Keys minted locally are never forwarded.
func (f *Forwarder) Targets(ctx context.Context, key pgwire.CancelKey) []string {
	id, ok := InstanceOf(key)
	if ok && id == f.cfg.Self {
		return nil
	}
	if ok {
		if addr, known := f.cfg.Static[id]; known {
			return []string{addr}
		}
		return f.discovered(ctx)
	}
	set := map[string]struct{}{}
	for _, addr := range f.cfg.Static {
		set[addr] = struct{}{}
	}
	for _, addr := range f.discovered(ctx) {
		set[addr] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for addr := range set {
		out = append(out, addr)
	}
	sort.Strings(out)
	return out
}

func (f *Forwarder) discovered(ctx context.Context) []string {
	if f.cfg.Service == "" {
		return nil
	}
	host, port, err := net.SplitHostPort(f.cfg.Service)
	if err != nil {
		f.cfg.Logger.Warn("peer service is not host:port", "service", f.cfg.Service, "err", err)
		return nil
	}
	ips, err := f.cfg.Resolve(ctx, host)
	if err != nil {
		f.cfg.Logger.Warn("peer discovery failed", "service", f.cfg.Service, "err", err)
		return nil
	}
	sort.Strings(ips)
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		out = append(out, net.JoinHostPort(ip, port))
	}
	return out
}

// Forward delivers key to its targets, best effort and rate limited.
func (f *Forwarder) Forward(ctx context.Context, key pgwire.CancelKey) {
	targets := f.Targets(ctx, key)
	if len(targets) == 0 {
		return
	}
	if !f.limiter.take(time.Now()) {
		f.mu.Lock()
		f.dropped++
		f.mu.Unlock()
		f.cfg.Logger.Warn("peer cancel dropped by rate limit", "pid", key.PID)
		return
	}
	req := &pgshardv1.RouterCancelRequest{Pid: key.PID, Secret: key.Secret}
	for _, addr := range targets {
		client, err := f.cfg.Dial(addr)
		if err != nil {
			f.cfg.Logger.Warn("peer cancel dial failed", "peer", addr, "err", err)
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, f.cfg.Timeout)
		_, err = client.Cancel(cctx, req)
		cancel()
		if err != nil {
			f.cfg.Logger.Warn("peer cancel failed", "peer", addr, "err", err)
		}
	}
}

// Dropped reports how many cancels the rate limit refused.
func (f *Forwarder) Dropped() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dropped
}

func (f *Forwarder) dial(addr string) (pgshardv1.RouterPeerClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cc, ok := f.conns[addr]
	if !ok {
		var err error
		cc, err = grpc.NewClient(addr, grpc.WithTransportCredentials(f.cfg.Creds))
		if err != nil {
			return nil, err
		}
		f.conns[addr] = cc
	}
	return pgshardv1.NewRouterPeerClient(cc), nil
}

// Close releases peer connections.
func (f *Forwarder) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for addr, cc := range f.conns {
		_ = cc.Close()
		delete(f.conns, addr)
	}
}

// bucket is a token bucket refilled continuously.
type bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(rate float64, burst int) *bucket {
	return &bucket{rate: rate, burst: float64(burst), tokens: float64(burst)}
}

func (b *bucket) take(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.last.IsZero() {
		b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Server answers RouterPeer.Cancel by cancelling a local session; keys that
// do not match a local session are ignored and never forwarded again, which
// keeps a misdirected cancel from bouncing between instances.
type Server struct {
	pgshardv1.UnimplementedRouterPeerServer
	Local func(ctx context.Context, key pgwire.CancelKey) bool
}

// Cancel implements pgshardv1.RouterPeerServer.
func (s *Server) Cancel(ctx context.Context, req *pgshardv1.RouterCancelRequest) (*pgshardv1.RouterCancelResponse, error) {
	if s.Local != nil {
		s.Local(ctx, pgwire.CancelKey{PID: req.GetPid(), Secret: req.GetSecret()})
	}
	return &pgshardv1.RouterCancelResponse{}, nil
}
