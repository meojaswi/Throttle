// Example server: Redis-backed limit shared across instances, with a local
// in-memory fallback if Redis goes down, and graceful shutdown.
//
//	REDIS_ADDR=localhost:6379 go run ./cmd/server
//	for i in $(seq 1 15); do curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/api/hello; done
//	for i in $(seq 1 5); do curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/api/otp; done
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/example/ratelimiter/limiter"
	"github.com/example/ratelimiter/middleware"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	rule := limiter.Rule{Rate: 5, Burst: 10} // 5 req/s steady, bursts of 10

	rdb := redis.NewClient(&redis.Options{
		Addr:         getenv("REDIS_ADDR", "localhost:6379"),
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
		PoolSize:     50,
	})
	defer rdb.Close()

	primary, err := limiter.NewRedis(rdb, rule, "rl:api:")
	must(err)
	local, err := limiter.NewMemory(rule)
	must(err)
	defer local.Close()

	lim := &limiter.Fallback{
		Primary:   primary,
		Secondary: local,
		OnError:   func(err error) { log.Warn("redis limiter down, using local fallback", "err", err) },
	}

	rl := middleware.RateLimit(middleware.Options{
		Limiter:  lim,
		KeyFunc:  middleware.KeyByIP,
		FailOpen: true,
		Logger:   log,
	})

	// OTP: hard quota (3 per rolling minute, no bursts) via the sliding
	// window counter, and fail closed — never send unlimited OTPs.
	otpRule := limiter.WindowRule{Limit: 3, Window: time.Minute}
	otpPrimary, err := limiter.NewRedisWindow(rdb, otpRule, "rl:otp:")
	must(err)
	otpLocal, err := limiter.NewMemoryWindow(otpRule)
	must(err)
	defer otpLocal.Close()

	otpRL := middleware.RateLimit(middleware.Options{
		Limiter: &limiter.Fallback{
			Primary:   otpPrimary,
			Secondary: otpLocal,
			OnError:   func(err error) { log.Warn("redis otp limiter down, using local fallback", "err", err) },
		},
		KeyFunc:  middleware.KeyByIP,
		FailOpen: false,
		Logger:   log,
	})

	mux := http.NewServeMux()
	mux.Handle("POST /api/otp", otpRL(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("otp sent\n"))
	})))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /api/hello", rl(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("hello\n"))
	})))

	srv := &http.Server{
		Addr:              ":" + getenv("PORT", "8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
