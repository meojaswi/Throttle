// Package middleware wires a limiter.Limiter into net/http.
package middleware

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/example/ratelimiter/limiter"
)

// KeyFunc extracts the identity to limit on (IP, API key, user ID...).
// Returning ErrSkip lets the request through unlimited (e.g. health checks).
type KeyFunc func(r *http.Request) (string, error)

// ErrSkip tells the middleware not to rate limit this request.
var ErrSkip = errors.New("skip rate limit")

// Options configures the middleware.
type Options struct {
	Limiter limiter.Limiter
	KeyFunc KeyFunc
	// Cost per request; nil means 1. Lets heavy endpoints consume more tokens.
	Cost func(r *http.Request) int
	// Timeout for one limiter call (default 50ms). Keeps a slow Redis from
	// adding latency to every request.
	Timeout time.Duration
	// FailOpen: if the limiter errors, allow the request (true) or reply 503
	// (false). Most public APIs fail open; auth/OTP endpoints should fail closed.
	FailOpen bool
	Logger   *slog.Logger
}

// RateLimit returns middleware that enforces the limit and sets the standard
// RateLimit-* and Retry-After headers.
func RateLimit(opts Options) func(http.Handler) http.Handler {
	if opts.Limiter == nil || opts.KeyFunc == nil {
		panic("middleware: Limiter and KeyFunc are required")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 50 * time.Millisecond
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, err := opts.KeyFunc(r)
			if errors.Is(err, ErrSkip) {
				next.ServeHTTP(w, r)
				return
			}
			if err != nil || key == "" {
				http.Error(w, "unable to identify client", http.StatusBadRequest)
				return
			}

			cost := 1
			if opts.Cost != nil {
				cost = opts.Cost(r)
			}

			ctx, cancel := context.WithTimeout(r.Context(), opts.Timeout)
			res, err := opts.Limiter.Allow(ctx, key, cost)
			cancel()
			if err != nil {
				opts.Logger.Error("rate limiter error", "err", err, "key", key)
				if opts.FailOpen {
					next.ServeHTTP(w, r)
					return
				}
				http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
				return
			}

			h := w.Header()
			h.Set("RateLimit-Limit", strconv.Itoa(res.Limit))
			h.Set("RateLimit-Remaining", strconv.Itoa(res.Remaining))
			h.Set("RateLimit-Reset", ceilSeconds(res.ResetAfter))

			if !res.Allowed {
				h.Set("Retry-After", ceilSeconds(res.RetryAfter))
				http.Error(w, "too many requests", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func ceilSeconds(d time.Duration) string {
	return strconv.Itoa(int(math.Ceil(d.Seconds())))
}

// KeyByIP limits per client IP using the TCP peer address. It deliberately
// ignores X-Forwarded-For: any client can forge that header. Behind a load
// balancer use KeyByTrustedProxyIP instead.
func KeyByIP(r *http.Request) (string, error) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "", err
	}
	return "ip:" + host, nil
}

// KeyByTrustedProxyIP reads X-Forwarded-For only when the request came from
// one of your own proxies, and then takes the right-most address that is not
// a trusted proxy — the real client as seen by your edge.
func KeyByTrustedProxyIP(trusted []*net.IPNet) KeyFunc {
	isTrusted := func(ip net.IP) bool {
		for _, n := range trusted {
			if n.Contains(ip) {
				return true
			}
		}
		return false
	}
	return func(r *http.Request) (string, error) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return "", err
		}
		peer := net.ParseIP(host)
		if peer == nil || !isTrusted(peer) {
			return "ip:" + host, nil
		}
		parts := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
		for i := len(parts) - 1; i >= 0; i-- {
			ip := net.ParseIP(strings.TrimSpace(parts[i]))
			if ip != nil && !isTrusted(ip) {
				return "ip:" + ip.String(), nil
			}
		}
		return "ip:" + host, nil
	}
}

// KeyByHeader limits per value of a header such as "X-API-Key".
// Requests without the header are rejected with 400.
func KeyByHeader(name string) KeyFunc {
	return func(r *http.Request) (string, error) {
		v := r.Header.Get(name)
		if v == "" {
			return "", errors.New("missing " + name)
		}
		return "hdr:" + v, nil
	}
}
