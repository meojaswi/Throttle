package limiter

import (
	"context"
	"sync/atomic"
	"time"
)

// Fallback tries Primary (usually Redis) and, if it errors, answers from
// Secondary (usually Memory). During a Redis outage you keep *some*
// protection — a per-instance limit — instead of either blocking every
// request (fail-closed) or letting all traffic through (fail-open).
//
// It also acts as a simple circuit breaker: after a Primary error it skips
// Primary for Cooldown, so an outage doesn't add a timeout's worth of
// latency to every single request.
type Fallback struct {
	Primary   Limiter
	Secondary Limiter
	Cooldown  time.Duration // default 2s
	OnError   func(error)   // hook for logs/metrics; may be nil

	openUntil atomic.Int64 // unix nanos; Primary is skipped until then
}

// Allow implements Limiter.
func (f *Fallback) Allow(ctx context.Context, key string, cost int) (Result, error) {
	if time.Now().UnixNano() >= f.openUntil.Load() {
		res, err := f.Primary.Allow(ctx, key, cost)
		if err == nil {
			return res, nil
		}
		cd := f.Cooldown
		if cd <= 0 {
			cd = 2 * time.Second
		}
		f.openUntil.Store(time.Now().Add(cd).UnixNano())
		if f.OnError != nil {
			f.OnError(err)
		}
	}
	return f.Secondary.Allow(ctx, key, cost)
}
