package limiter

import (
	"errors"
	"math"
	"time"
)

// WindowRule describes a sliding-window limit: at most Limit units of cost
// in any rolling Window. Example: WindowRule{Limit: 3, Window: time.Minute}
// = at most 3 OTP requests per minute, with no burst allowance.
//
// Use it instead of the token bucket (Rule) when you need a hard quota per
// period ("1000 calls per hour") rather than a steady rate with bursts.
type WindowRule struct {
	Limit  int
	Window time.Duration
}

func (r WindowRule) validate() error {
	if r.Limit <= 0 {
		return errors.New("limiter: Limit must be > 0")
	}
	if r.Window <= 0 {
		return errors.New("limiter: Window must be > 0")
	}
	return nil
}

// windowDecision is the sliding window counter algorithm. Time is cut into
// fixed windows; we keep only two counters (previous and current window) and
// estimate the count in the rolling window as
//
//	prev * (fraction of previous window still inside the rolling window) + curr
//
// This removes the fixed-window bug where a client sends Limit requests at
// the end of one window and Limit more at the start of the next (2x burst),
// while using O(1) memory per key instead of storing every timestamp.
//
// prev, curr: counts in the previous and current fixed window.
// elapsed: seconds since the current window started. w: window length in seconds.
//
// The Lua script in redis_window.go mirrors this function line for line;
// change both together.
func windowDecision(prev, curr, elapsed, w float64, limit, cost int) (allowed bool, newCurr, remaining, retry, reset float64) {
	lim, c := float64(limit), float64(cost)
	prevWeighted := prev * (1 - elapsed/w)
	est := prevWeighted + curr

	if est+c <= lim {
		allowed = true
		curr += c
		est += c
	} else {
		// How long until est+cost fits under the limit?
		need := est + c - lim
		tail := w - elapsed
		switch {
		case prev > 0 && prevWeighted >= need:
			// The previous window's share drains at prev/w per second.
			retry = need * w / prev
		case curr+c-lim <= curr && curr > 0:
			// Wait for this window to end; then curr becomes the draining "prev".
			retry = tail + (curr+c-lim)*w/curr
		default:
			// cost > limit: it can never fit. Report a full cycle.
			retry = tail + w
		}
	}

	remaining = math.Max(0, lim-est)
	switch {
	case curr > 0:
		reset = (w - elapsed) + w
	case prevWeighted > 0:
		reset = w - elapsed
	}
	return allowed, curr, remaining, retry, reset
}
