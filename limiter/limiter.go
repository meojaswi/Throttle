// Package limiter provides token-bucket rate limiters: an in-memory one for a
// single instance and a Redis-backed one for many instances sharing a limit.
package limiter

import (
	"context"
	"errors"
	"time"
)

// Rule describes one limit: Rate tokens are added per second, up to Burst.
// Example: Rule{Rate: 10, Burst: 20} = steady 10 req/s, short spikes up to 20.
type Rule struct {
	Rate  float64 // tokens refilled per second
	Burst int     // bucket capacity (max requests in a spike)
}

func (r Rule) validate() error {
	if r.Rate <= 0 {
		return errors.New("limiter: Rate must be > 0")
	}
	if r.Burst <= 0 {
		return errors.New("limiter: Burst must be > 0")
	}
	return nil
}

// Result is the decision for one request.
type Result struct {
	Allowed    bool
	Limit      int           // = Burst
	Remaining  int           // whole tokens left after this request
	RetryAfter time.Duration // when Allowed=false: wait this long before retrying
	ResetAfter time.Duration // time until the bucket is completely full again
}

// Limiter decides whether a request identified by key may proceed.
// cost is usually 1; expensive endpoints can charge more.
type Limiter interface {
	Allow(ctx context.Context, key string, cost int) (Result, error)
}
