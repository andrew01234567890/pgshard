package cancelpeer

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

func key32(instance, pid uint32) pgwire.CancelKey {
	secret := make([]byte, pgwire.CancelKeyLen32)
	binary.BigEndian.PutUint32(secret[0:4], instance)
	binary.BigEndian.PutUint32(secret[4:8], pid)
	return pgwire.CancelKey{PID: pid, Secret: secret}
}

func key30(pid uint32) pgwire.CancelKey {
	return pgwire.CancelKey{PID: pid, Secret: []byte{1, 2, 3, 4}}
}

func TestInstanceOf(t *testing.T) {
	if id, ok := InstanceOf(key32(0xCAFE, 9)); !ok || id != 0xCAFE {
		t.Fatalf("got %x %v", id, ok)
	}
	if _, ok := InstanceOf(key30(9)); ok {
		t.Fatal("3.0 keys carry no instance")
	}
}

type recorder struct {
	mu    sync.Mutex
	calls map[string][]*pgshardv1.RouterCancelRequest
	fail  bool
}

type recClient struct {
	r    *recorder
	addr string
}

func (c recClient) Cancel(_ context.Context, req *pgshardv1.RouterCancelRequest, _ ...grpc.CallOption) (*pgshardv1.RouterCancelResponse, error) {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	if c.r.calls == nil {
		c.r.calls = map[string][]*pgshardv1.RouterCancelRequest{}
	}
	c.r.calls[c.addr] = append(c.r.calls[c.addr], req)
	if c.r.fail {
		return nil, errors.New("boom")
	}
	return &pgshardv1.RouterCancelResponse{}, nil
}

func (r *recorder) dial(addr string) (pgshardv1.RouterPeerClient, error) {
	return recClient{r: r, addr: addr}, nil
}

func (r *recorder) count(addr string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls[addr])
}

func newForwarder(t *testing.T, cfg Config, r *recorder) *Forwarder {
	t.Helper()
	cfg.Dial = r.dial
	f, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestTargets(t *testing.T) {
	static := map[uint32]string{2: "b:1", 3: "c:1"}
	resolve := func(_ context.Context, host string) ([]string, error) {
		if host != "routers" {
			return nil, errors.New("unknown host")
		}
		return []string{"10.0.0.2", "10.0.0.1"}, nil
	}
	ctx := context.Background()
	cases := []struct {
		name    string
		cfg     Config
		key     pgwire.CancelKey
		targets []string
	}{
		{"3.2 key for self", Config{Self: 1, Static: static}, key32(1, 5), nil},
		{"3.2 key for known peer", Config{Self: 1, Static: static}, key32(3, 5), []string{"c:1"}},
		{"3.2 key for unknown peer without discovery", Config{Self: 1, Static: static}, key32(9, 5), nil},
		{"3.2 key for unknown peer with discovery", Config{Self: 1, Static: static, Service: "routers:7000", Resolve: resolve}, key32(9, 5), []string{"10.0.0.1:7000", "10.0.0.2:7000"}},
		{"3.0 key goes everywhere", Config{Self: 1, Static: static, Service: "routers:7000", Resolve: resolve}, key30(5), []string{"10.0.0.1:7000", "10.0.0.2:7000", "b:1", "c:1"}},
		{"3.0 key with no peers", Config{Self: 1}, key30(5), nil},
		{"discovery failure is empty", Config{Self: 1, Service: "nope:7000", Resolve: resolve}, key30(5), nil},
		{"service without port is empty", Config{Self: 1, Service: "routers", Resolve: resolve}, key30(5), nil},
	}
	for _, c := range cases {
		f := newForwarder(t, c.cfg, &recorder{})
		got := f.Targets(ctx, c.key)
		if len(got) == 0 && len(c.targets) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, c.targets) {
			t.Errorf("%s: got %v want %v", c.name, got, c.targets)
		}
	}
}

func TestForwardDeliversKeyVerbatim(t *testing.T) {
	r := &recorder{}
	f := newForwarder(t, Config{Self: 1, Static: map[uint32]string{1: "a:1", 2: "b:1"}}, r)
	key := key32(2, 77)
	f.Forward(context.Background(), key)
	if r.count("b:1") != 1 {
		t.Fatalf("calls %v", r.calls)
	}
	got := r.calls["b:1"][0]
	if got.Pid != 77 || string(got.Secret) != string(key.Secret) {
		t.Fatalf("forwarded %v", got)
	}
	f.Forward(context.Background(), key32(1, 77))
	if r.count("b:1") != 1 || r.count("a:1") != 0 {
		t.Fatal("local key was forwarded")
	}
	f.Forward(context.Background(), key32(5, 77))
	if r.count("b:1") != 1 {
		t.Fatal("unknown instance was forwarded")
	}
}

func TestForwardRateLimit(t *testing.T) {
	r := &recorder{}
	f := newForwarder(t, Config{Self: 1, Static: map[uint32]string{2: "b:1"}, Rate: 0.001, Burst: 3}, r)
	for i := 0; i < 5; i++ {
		f.Forward(context.Background(), key32(2, uint32(i)))
	}
	if r.count("b:1") != 3 || f.Dropped() != 2 {
		t.Fatalf("delivered %d dropped %d", r.count("b:1"), f.Dropped())
	}
	// A key with no target consumes no token.
	f.Forward(context.Background(), key32(1, 1))
	if f.Dropped() != 2 {
		t.Fatal("local key charged the limiter")
	}
}

func TestBucketRefills(t *testing.T) {
	b := newBucket(10, 1)
	now := time.Unix(0, 0)
	if !b.take(now) || b.take(now) {
		t.Fatal("burst of one")
	}
	if b.take(now.Add(50 * time.Millisecond)) {
		t.Fatal("refilled too early")
	}
	if !b.take(now.Add(150 * time.Millisecond)) {
		t.Fatal("did not refill after 100ms at 10/s")
	}
}

func TestForwardToleratesPeerFailure(t *testing.T) {
	r := &recorder{fail: true}
	f := newForwarder(t, Config{Self: 1, Static: map[uint32]string{2: "b:1", 3: "c:1"}}, r)
	f.Forward(context.Background(), key30(4))
	if r.count("b:1") != 1 || r.count("c:1") != 1 {
		t.Fatalf("a failing peer must not stop delivery to the others: %v", r.calls)
	}
}

func TestNewRequiresCredentialsForRealPeers(t *testing.T) {
	if _, err := New(Config{Static: map[uint32]string{2: "b:1"}}); err == nil {
		t.Fatal("peers without credentials accepted")
	}
	if _, err := New(Config{}); err != nil {
		t.Fatalf("no peers needs no credentials: %v", err)
	}
}

func TestServerCancelsLocallyOnly(t *testing.T) {
	var got []pgwire.CancelKey
	s := &Server{Local: func(_ context.Context, key pgwire.CancelKey) bool { got = append(got, key); return false }}
	if _, err := s.Cancel(context.Background(), &pgshardv1.RouterCancelRequest{Pid: 3, Secret: []byte{9}}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PID != 3 || string(got[0].Secret) != "\x09" {
		t.Fatalf("local got %v", got)
	}
	if _, err := (&Server{}).Cancel(context.Background(), &pgshardv1.RouterCancelRequest{}); err != nil {
		t.Fatal(err)
	}
}
