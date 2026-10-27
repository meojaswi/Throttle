package limiter

import (
	"context"
	"math"
	"sync"
	"time"
)

type windowEntry struct {
	idx  int64 // which fixed window curr belongs to (unixNanos / window)
	curr float64
	prev float64
}

type windowShard struct {
	mu      sync.Mutex
	entries map[string]*windowEntry
}

// MemoryWindow is an in-process sliding window counter limiter. Like Memory,
// keys are spread over 64 mutex-protected shards and idle keys are evicted
// in the background.
type MemoryWindow struct {
	rule   WindowRule
	shards [shardCount]*windowShard
	now    func() time.Time
	stop   chan struct{}
	once   sync.Once
}

// WindowOption configures a MemoryWindow limiter.
type WindowOption func(*MemoryWindow)

// WithWindowClock overrides time.Now (used in tests).
func WithWindowClock(now func() time.Time) WindowOption {
	return func(m *MemoryWindow) { m.now = now }
}

// NewMemoryWindow builds an in-memory sliding window limiter. Call Close when
// done to stop the cleanup goroutine.
func NewMemoryWindow(rule WindowRule, opts ...WindowOption) (*MemoryWindow, error) {
	if err := rule.validate(); err != nil {
		return nil, err
	}
	m := &MemoryWindow{rule: rule, now: time.Now, stop: make(chan struct{})}
	for i := range m.shards {
		m.shards[i] = &windowShard{entries: make(map[string]*windowEntry)}
	}
	for _, o := range opts {
		o(m)
	}
	go m.janitor()
	return m, nil
}

func (m *MemoryWindow) position(now time.Time) (idx int64, elapsed float64) {
	w := int64(m.rule.Window)
	n := now.UnixNano()
	idx = n / w
	return idx, float64(n-idx*w) / float64(time.Second)
}

// Allow implements Limiter.
func (m *MemoryWindow) Allow(_ context.Context, key string, cost int) (Result, error) {
	if cost <= 0 {
		cost = 1
	}
	idx, elapsed := m.position(m.now())

	s := m.shards[shardIndex(key)]
	s.mu.Lock()
	e, ok := s.entries[key]
	if !ok {
		e = &windowEntry{idx: idx}
		s.entries[key] = e
	}
	// Roll the counters forward if we've moved into a new window.
	switch {
	case e.idx == idx-1:
		e.prev, e.curr = e.curr, 0
	case e.idx < idx-1:
		e.prev, e.curr = 0, 0
	}
	if e.idx < idx {
		e.idx = idx
	}

	allowed, curr, remaining, retry, reset := windowDecision(
		e.prev, e.curr, elapsed, m.rule.Window.Seconds(), m.rule.Limit, cost)
	e.curr = curr
	s.mu.Unlock()

	return Result{
		Allowed:    allowed,
		Limit:      m.rule.Limit,
		Remaining:  int(math.Floor(remaining)),
		RetryAfter: secs(retry),
		ResetAfter: secs(reset),
	}, nil
}

// janitor deletes entries older than two windows: both of their counters
// have fully slid out, so they are identical to a brand-new key.
func (m *MemoryWindow) janitor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			idx, _ := m.position(m.now())
			for _, s := range m.shards {
				s.mu.Lock()
				for k, e := range s.entries {
					if e.idx < idx-1 {
						delete(s.entries, k)
					}
				}
				s.mu.Unlock()
			}
		}
	}
}

// Close stops the background cleanup goroutine. Safe to call more than once.
func (m *MemoryWindow) Close() error {
	m.once.Do(func() { close(m.stop) })
	return nil
}
