package pooler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// BenchmarkRelayConcurrentSessions is fan-in through the pooler: a stream
// and a logical session each, all relaying at once. ns/op is wall time per
// message across every session, so it falls as concurrency rises while the
// server keeps up; run it with -mutexprofile to see what the server-wide
// mutex costs, which is what makes this benchmark worth having.
func BenchmarkRelayConcurrentSessions(b *testing.B) {
	for _, sessions := range []int{1, 8, 64} {
		b.Run(fmt.Sprint(sessions), func(b *testing.B) {
			h := startHarness(b, PoolConfig{MaxBackends: sessions, MaxPerRole: sessions, AcquireTimeout: 5 * time.Second})
			ctx := context.Background()
			streams := make([]pgshardv1.Pooler_ExecuteClient, sessions)
			for i := range streams {
				st, err := h.client.Execute(ctx)
				if err != nil {
					b.Fatal(err)
				}
				streams[i] = st
			}
			b.ResetTimer()
			var wg sync.WaitGroup
			per := b.N / sessions
			if per == 0 {
				per = 1
			}
			for i := range streams {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					sid := fmt.Sprintf("s-%d", i)
					for n := 0; n < per; n++ {
						if err := streams[i].Send(queryReq(sid, "select 1", gen(7, 3), identity("alice"))); err != nil {
							return
						}
						for {
							resp, err := streams[i].Recv()
							if err != nil {
								return
							}
							if resp.GetReadyForQuery() != nil {
								break
							}
						}
					}
				}(i)
			}
			wg.Wait()
			b.StopTimer()
			for _, st := range streams {
				_ = st.CloseSend()
			}
		})
	}
}
