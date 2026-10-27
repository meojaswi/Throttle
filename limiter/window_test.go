package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// windowStart is an exact multiple of one minute, so fixed windows line up.
var windowStart = time.Unix(6000, 0)

func TestWindowMemoryLimitAndRetry(t *testing.T) {
	clk := &fakeClock{t: windowStart}
	m, err := NewMemoryWindow(WindowRule{Limit: 10, Window: time.Minute}, WithWindowClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if r, _ := m.Allow(ctx, "u1", 1); !r.Allowed {
			t.Fatalf("request %d should pass", i)
		}
	}
	r, _ := m.Allow(ctx, "u1", 1)
	if r.Allowed {
		t.Fatal("11th request should be limited")
	}
	if r.Remaining != 0 || r.Limit != 10 {
		t.Fatalf("bad result %+v", r)
	}
	// Next window starts in 60s; then the 10 old requests drain at 10/min,
	// so one slot opens 6s later.
	if r.RetryAfter != 66*time.Second {
		t.Fatalf("RetryAfter = %v, want 66s", r.RetryAfter)
	}

	if r, _ := m.Allow(ctx, "u2", 1); !r.Allowed {
		t.Fatal("u2 must have its own counter")
	}

	clk.Advance(r.RetryAfter)
	if r, _ := m.Allow(ctx, "u1", 1); !r.Allowed {
		t.Fatalf("should pass after RetryAfter, got %+v", r)
	}
}

// The reason this algorithm exists: a fixed window would allow 10 requests at
// 0:59 and 10 more at 1:01. The sliding window must not.
func TestWindowMemoryNoBoundaryBurst(t *testing.T) {
	clk := &fakeClock{t: windowStart.Add(59 * time.Second)}
	m, _ := NewMemoryWindow(WindowRule{Limit: 10, Window: time.Minute}, WithWindowClock(clk.Now))
	defer m.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		m.Allow(ctx, "k", 1)
	}
	clk.Advance(2 * time.Second) // 1s into the next fixed window
	if r, _ := m.Allow(ctx, "k", 1); r.Allowed {
		t.Fatal("boundary burst allowed; sliding window should still count the previous window")
	}

	clk.Advance(6 * time.Second) // 7s in: estimate = 10*(53/60) ≈ 8.8
	if r, _ := m.Allow(ctx, "k", 1); !r.Allowed {
		t.Fatalf("one slot should have opened, got %+v", r)
	}

	clk.Advance(2 * time.Minute) // both windows slid out
	r, _ := m.Allow(ctx, "k", 1)
	if !r.Allowed || r.Remaining != 9 {
		t.Fatalf("counter should be fresh, got %+v", r)
	}
}

func TestWindowMemoryConcurrent(t *testing.T) {
	clk := &fakeClock{t: windowStart}
	m, _ := NewMemoryWindow(WindowRule{Limit: 50, Window: time.Hour}, WithWindowClock(clk.Now))
	defer m.Close()
	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r, _ := m.Allow(context.Background(), "hot", 1); r.Allowed {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 50 {
		t.Fatalf("allowed %d, want exactly 50", ok.Load())
	}
}

func TestWindowRedisSharedAcrossInstances(t *testing.T) {
	mr, rdb := newRedis(t)
	mr.SetTime(windowStart)
	a, _ := NewRedisWindow(rdb, WindowRule{Limit: 20, Window: time.Hour}, "rl:w:")
	b, _ := NewRedisWindow(rdb, WindowRule{Limit: 20, Window: time.Hour}, "rl:w:")

	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		lim := a
		if i%2 == 0 {
			lim = b
		}
		go func() {
			defer wg.Done()
			r, err := lim.Allow(context.Background(), "user:42", 1)
			if err != nil {
				t.Error(err)
				return
			}
			if r.Allowed {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 20 {
		t.Fatalf("allowed %d across instances, want exactly 20", ok.Load())
	}
}

func TestWindowRedisBoundaryRetryAndTTL(t *testing.T) {
	mr, rdb := newRedis(t)
	lim, _ := NewRedisWindow(rdb, WindowRule{Limit: 10, Window: time.Minute}, "rl:w:")
	ctx := context.Background()

	mr.SetTime(windowStart)
	for i := 0; i < 10; i++ {
		if r, err := lim.Allow(ctx, "k", 1); err != nil || !r.Allowed {
			t.Fatalf("request %d: %+v %v", i, r, err)
		}
	}
	r, err := lim.Allow(ctx, "k", 1)
	if err != nil || r.Allowed {
		t.Fatalf("11th should be limited, got %+v %v", r, err)
	}
	if r.RetryAfter != 66*time.Second || r.Remaining != 0 {
		t.Fatalf("Redis and Memory must agree; got %+v", r)
	}
	if ttl := mr.TTL("rl:w:k"); ttl <= 0 {
		t.Fatalf("key should have a TTL, got %v", ttl)
	}

	mr.SetTime(windowStart.Add(61 * time.Second)) // boundary: still blocked
	if r, _ := lim.Allow(ctx, "k", 1); r.Allowed {
		t.Fatal("boundary burst allowed in Redis")
	}
	mr.SetTime(windowStart.Add(67 * time.Second))
	if r, _ := lim.Allow(ctx, "k", 1); !r.Allowed {
		t.Fatalf("should pass after RetryAfter, got %+v", r)
	}
}

func TestWindowCost(t *testing.T) {
	clk := &fakeClock{t: windowStart}
	m, _ := NewMemoryWindow(WindowRule{Limit: 10, Window: time.Minute}, WithWindowClock(clk.Now))
	defer m.Close()
	ctx := context.Background()
	if r, _ := m.Allow(ctx, "k", 7); !r.Allowed || r.Remaining != 3 {
		t.Fatalf("cost 7: %+v", r)
	}
	if r, _ := m.Allow(ctx, "k", 5); r.Allowed {
		t.Fatal("cost 5 should not fit in 3 remaining")
	}
	if r, _ := m.Allow(ctx, "k", 3); !r.Allowed {
		t.Fatal("cost 3 should fit exactly")
	}
}

func TestInvalidWindowRule(t *testing.T) {
	if _, err := NewMemoryWindow(WindowRule{Limit: 0, Window: time.Second}); err == nil {
		t.Fatal("want error for zero limit")
	}
	if _, err := NewRedisWindow(nil, WindowRule{Limit: 1}, ""); err == nil {
		t.Fatal("want error for zero window")
	}
}
