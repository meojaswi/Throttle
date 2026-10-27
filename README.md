# Throttle

Distributed rate limiter for Go with two algorithms — token bucket and
sliding window counter — each Redis-backed (shared across instances) with an
in-memory fallback, plus net/http middleware.

## Algorithms

| | Token bucket (`Rule`) | Sliding window counter (`WindowRule`) |
|---|---|---|
| Limit means | Steady rate + allowed burst | Approximate cap per rolling window (no instant boundary burst) |
| Bursts | Yes, up to `Burst` | No |
| Good for | General API traffic | OTP/login, plan quotas ("1000/hour") |
| State per key | tokens, last timestamp | prev count, curr count, window index |
| Redis | `NewRedis` | `NewRedisWindow` |
| Memory | `NewMemory` | `NewMemoryWindow` |

The sliding window counter keeps only two counters per key and estimates the
rolling count as `prev × (unelapsed fraction of prev window) + curr`. This
fixes the fixed-window boundary bug (Limit requests at 0:59 + Limit more at
1:01) without storing every timestamp. Both implement the same `Limiter`
interface, so the middleware and `Fallback` work unchanged with either.

```
request ─► middleware ─► KeyFunc (IP / API key) ─► Fallback
                                                   ├─ Redis (Lua, atomic)   ← normal path
                                                   └─ Memory (per-instance) ← Redis down
           ◄─ 200 + RateLimit-* headers  |  429 + Retry-After
```

## Run

```bash
docker run -d -p 6379:6379 redis:7
go mod tidy
go test -race ./...
go run ./cmd/server
for i in $(seq 1 15); do curl -s -o /dev/null -w "%{http_code} " localhost:8080/api/hello; done
for i in $(seq 1 5);  do curl -s -o /dev/null -w "%{http_code} " -X POST localhost:8080/api/otp; done   # 200 200 200 429 429
```

## Use in your service

```go
rule := limiter.Rule{Rate: 5, Burst: 10}           // 5 req/s, bursts of 10
redisLim, _ := limiter.NewRedis(rdb, rule, "rl:api:")
localLim, _ := limiter.NewMemory(rule)
lim := &limiter.Fallback{Primary: redisLim, Secondary: localLim}

mw := middleware.RateLimit(middleware.Options{
    Limiter: lim, KeyFunc: middleware.KeyByIP, FailOpen: true,
})
mux.Handle("/api/", mw(apiHandler))
```

Per-window quota with the sliding window (fail closed for sensitive endpoints):

```go
otpRule := limiter.WindowRule{Limit: 3, Window: time.Minute}  // 3 per rolling minute
otpRedis, _ := limiter.NewRedisWindow(rdb, otpRule, "rl:otp:")
otpLocal, _ := limiter.NewMemoryWindow(otpRule)

otpMW := middleware.RateLimit(middleware.Options{
    Limiter:  &limiter.Fallback{Primary: otpRedis, Secondary: otpLocal},
    KeyFunc:  middleware.KeyByIP,
    FailOpen: false,
})
mux.Handle("POST /api/otp", otpMW(otpHandler))
```

Use a different key prefix for each limiter; the two algorithms store
different fields under their keys.

## Why it's production-grade

| Concern | How it's handled |
|---|---|
| Race between instances | Whole read-refill-write runs in one Lua script → Redis executes it atomically |
| Clock skew between servers | Script uses Redis `TIME`, not the app's clock |
| Memory growth | Redis keys get `PEXPIRE`; in-memory buckets evicted by a janitor |
| Lock contention (in-memory) | 64 sharded mutexes instead of one global lock |
| Redis outage | `Fallback` switches to a local limiter; circuit breaker skips Redis for `Cooldown` so requests don't each eat a timeout |
| Slow Redis | Per-call context timeout (default 50ms) + client read/write timeouts |
| IP spoofing via `X-Forwarded-For` | `KeyByIP` ignores it; `KeyByTrustedProxyIP` only trusts it from your own LB CIDRs |
| Client feedback | `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset`, `Retry-After` |
| Weighted endpoints | `Options.Cost` — e.g. a search costs 5 tokens, a GET costs 1 |
| Fail-open vs fail-closed | Configurable per route (login/OTP → fail closed) |

## Next steps for real deployment

- Add Prometheus counters in the middleware (`allowed_total`, `limited_total`, `limiter_errors_total`) and in `Fallback.OnError`.
- Different rules per route/plan: create one `Redis` limiter per rule with its own prefix (`rl:free:`, `rl:pro:`).
- Redis Cluster: pass a `*redis.ClusterClient`; each bucket is a single key so no cross-slot issues.
- Requires Redis 5+ (scripts call `TIME` before writing; default effect replication handles this).
