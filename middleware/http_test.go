package middleware

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/example/ratelimiter/limiter"
)

func handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
}

func TestMiddleware429AndHeaders(t *testing.T) {
	m, _ := limiter.NewMemory(limiter.Rule{Rate: 1, Burst: 2})
	defer m.Close()
	h := RateLimit(Options{Limiter: m, KeyFunc: KeyByIP})(handler())

	codes := []int{}
	var last *httptest.ResponseRecorder
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "1.2.3.4:5555"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
		last = rec
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 429 {
		t.Fatalf("codes = %v, want [200 200 429]", codes)
	}
	if last.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q", last.Header().Get("Retry-After"))
	}
	if last.Header().Get("RateLimit-Limit") != "2" || last.Header().Get("RateLimit-Remaining") != "0" {
		t.Fatalf("bad headers: %v", last.Header())
	}
}

type broken struct{}

func (broken) Allow(context.Context, string, int) (limiter.Result, error) {
	return limiter.Result{}, errors.New("redis down")
}

func TestFailOpenVsClosed(t *testing.T) {
	for _, tc := range []struct {
		failOpen bool
		want     int
	}{{true, 200}, {false, 503}} {
		h := RateLimit(Options{Limiter: broken{}, KeyFunc: KeyByIP, FailOpen: tc.failOpen})(handler())
		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("failOpen=%v: got %d want %d", tc.failOpen, rec.Code, tc.want)
		}
	}
}

func TestTrustedProxyIP(t *testing.T) {
	_, lb, _ := net.ParseCIDR("10.0.0.0/8")
	kf := KeyByTrustedProxyIP([]*net.IPNet{lb})

	// From the load balancer: use the real client from XFF.
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.5:1234"
	req.Header.Set("X-Forwarded-For", "6.6.6.6, 203.0.113.9, 10.0.0.7")
	if k, _ := kf(req); k != "ip:203.0.113.9" {
		t.Fatalf("got %q", k)
	}

	// From the internet: a forged XFF header must be ignored.
	req.RemoteAddr = "198.51.100.1:1234"
	if k, _ := kf(req); k != "ip:198.51.100.1" {
		t.Fatalf("spoofed XFF was trusted: %q", k)
	}
}
