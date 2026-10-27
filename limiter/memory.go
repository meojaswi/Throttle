package limiter

import (
	"context"
	"hash/fnv"
	"math"
	"sync"
	"time"
)

const shardCount = 64

type bucket struct {
	tokens float64
	last   time.Time
}

type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

// Memory is an in-process token-bucket limiter. Keys are spread across
// 64 shards, each with its own mutex, so unrelated keys don't contend for
// one global lock. Idle buckets are evicted in the background so memory
// doesn't grow without bound when you limit by IP.
type Memory struct {
	rule   Rule
	shards [shardCount]*shard
	now    func() time.Time
	stop   chan struct{}
	once   sync.Once
}

// MemoryOption configures a Memory limiter.
type MemoryOption func(*Memory)

// WithClock overrides time.Now (used in tests).
func WithClock(now func() time.Time) MemoryOption { return func(m *Memory) { m.now = now } }

// NewMemory builds an in-memory limiter. Call Close when done to stop the
// cleanup goroutine.
func NewMemory(rule Rule, opts ...MemoryOption) (*Memory, error) {
	if err := rule.validate(); err != nil {
		return nil, err
	}
	m := &Memory{rule: rule, now: time.Now, stop: make(chan struct{})}
	for i := range m.shards {
		m.shards[i] = &shard{buckets: make(map[string]*bucket)}
	}
	for _, o := range opts {
		o(m)
	}
	go m.janitor(m.fullRefill() + time.Minute)
	return m, nil
}

// fullRefill is how long an empty bucket takes to become full. After that an
// idle bucket is indistinguishable from a fresh one, so it can be deleted.
func (m *Memory) fullRefill() time.Duration {
	return time.Duration(float64(m.rule.Burst) / m.rule.Rate * float64(time.Second))
}

// shardIndex picks one of the 64 shards for a key (shared by Memory and
// MemoryWindow).
func shardIndex(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % shardCount
}

func (m *Memory) shardFor(key string) *shard {
	return m.shards[shardIndex(key)]
}

// Allow implements Limiter.
func (m *Memory) Allow(_ context.Context, key string, cost int) (Result, error) {
	if cost <= 0 {
		cost = 1
	}
	now := m.now()
	burst := float64(m.rule.Burst)

	s := m.shardFor(key)
	s.mu.Lock()
	b, ok := s.buckets[key]
	if !ok {
		b = &bucket{tokens: burst, last: now}
		s.buckets[key] = b
	}
	// Refill based on time elapsed since the last request.
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(burst, b.tokens+elapsed*m.rule.Rate)
		b.last = now
	}

	res := Result{Limit: m.rule.Burst}
	if b.tokens >= float64(cost) {
		b.tokens -= float64(cost)
		res.Allowed = true
	} else {
		res.RetryAfter = secs((float64(cost) - b.tokens) / m.rule.Rate)
	}
	res.Remaining = int(math.Floor(b.tokens))
	res.ResetAfter = secs((burst - b.tokens) / m.rule.Rate)
	s.mu.Unlock()
	return res, nil
}

func (m *Memory) janitor(idle time.Duration) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			cutoff := m.now().Add(-idle)
			for _, s := range m.shards {
				s.mu.Lock()
				for k, b := range s.buckets {
					if b.last.Before(cutoff) {
						delete(s.buckets, k)
					}
				}
				s.mu.Unlock()
			}
		}
	}
}

// Close stops the background cleanup goroutine. Safe to call more than once.
func (m *Memory) Close() error {
	m.once.Do(func() { close(m.stop) })
	return nil
}

func secs(f float64) time.Duration { return time.Duration(f * float64(time.Second)) }
