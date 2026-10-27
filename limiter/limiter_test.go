package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func TestMemoryBurstThenRefill(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	m, err := NewMemory(Rule{Rate: 2, Burst: 5}, WithClock(clk.Now))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if r, _ := m.Allow(ctx, "u1", 1); !r.Allowed {
			t.Fatalf("request %d should pass", i)
		}
	}
	r, _ := m.Allow(ctx, "u1", 1)
	if r.Allowed {
		t.Fatal("6th request should be limited")
	}
	if r.RetryAfter != 500*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 500ms", r.RetryAfter)
	}

	// Other keys are independent.
	if r, _ := m.Allow(ctx, "u2", 1); !r.Allowed {
		t.Fatal("u2 must have its own bucket")
	}

	clk.Advance(time.Second) // +2 tokens
	for i := 0; i < 2; i++ {
		if r, _ := m.Allow(ctx, "u1", 1); !r.Allowed {
			t.Fatalf("refilled request %d should pass", i)
		}
	}
	if r, _ := m.Allow(ctx, "u1", 1); r.Allowed {
		t.Fatal("bucket should be empty again")
	}
}

func TestMemoryConcurrent(t *testing.T) {
	m, _ := NewMemory(Rule{Rate: 0.001, Burst: 50})
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

func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return mr, rdb
}

func TestRedisAtomicAcrossInstances(t *testing.T) {
	_, rdb := newRedis(t)
	// Two "app instances" sharing one Redis must share one limit.
	a, _ := NewRedis(rdb, Rule{Rate: 0.001, Burst: 20}, "rl:")
	b, _ := NewRedis(rdb, Rule{Rate: 0.001, Burst: 20}, "rl:")

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

func TestRedisRefillAndTTL(t *testing.T) {
	mr, rdb := newRedis(t)
	lim, _ := NewRedis(rdb, Rule{Rate: 10, Burst: 3}, "rl:")
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		lim.Allow(ctx, "k", 1)
	}
	r, _ := lim.Allow(ctx, "k", 1)
	if r.Allowed || r.RetryAfter <= 0 {
		t.Fatalf("want limited with RetryAfter>0, got %+v", r)
	}
	if ttl := mr.TTL("rl:k"); ttl <= 0 {
		t.Fatalf("key should have a TTL, got %v", ttl)
	}

	time.Sleep(150 * time.Millisecond) // ~1.5 tokens back at 10/s
	if r, _ := lim.Allow(ctx, "k", 1); !r.Allowed {
		t.Fatalf("should pass after refill, got %+v", r)
	}
}

func TestFallbackWhenRedisDown(t *testing.T) {
	mr, rdb := newRedis(t)
	primary, _ := NewRedis(rdb, Rule{Rate: 1, Burst: 2}, "rl:")
	local, _ := NewMemory(Rule{Rate: 0.001, Burst: 2})
	defer local.Close()

	var gotErr error
	f := &Fallback{Primary: primary, Secondary: local, OnError: func(e error) { gotErr = e }}
	mr.Close() // simulate outage

	ctx := context.Background()
	r1, err := f.Allow(ctx, "k", 1)
	if err != nil || !r1.Allowed {
		t.Fatalf("fallback should answer, got %+v %v", r1, err)
	}
	f.Allow(ctx, "k", 1)
	if r, _ := f.Allow(ctx, "k", 1); r.Allowed {
		t.Fatal("local fallback should still enforce the limit")
	}
	if gotErr == nil {
		t.Fatal("OnError hook should fire")
	}
}

func TestInvalidRule(t *testing.T) {
	if _, err := NewMemory(Rule{Rate: 0, Burst: 1}); err == nil {
		t.Fatal("want error for zero rate")
	}
	if _, err := NewRedis(nil, Rule{Rate: 1, Burst: 0}, ""); err == nil {
		t.Fatal("want error for zero burst")
	}
}

type countingErr struct{ n atomic.Int64 }

func (c *countingErr) Allow(context.Context, string, int) (Result, error) {
	c.n.Add(1)
	return Result{}, context.DeadlineExceeded
}

func TestFallbackCircuitBreaker(t *testing.T) {
	p := &countingErr{}
	local, _ := NewMemory(Rule{Rate: 1000, Burst: 1000})
	defer local.Close()
	f := &Fallback{Primary: p, Secondary: local, Cooldown: time.Hour}
	for i := 0; i < 100; i++ {
		f.Allow(context.Background(), "k", 1)
	}
	if p.n.Load() != 1 {
		t.Fatalf("primary called %d times during cooldown, want 1", p.n.Load())
	}
}
