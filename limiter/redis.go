package limiter

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// tokenBucketScript runs atomically inside Redis, so concurrent requests from
// many app instances can never double-spend the same token (no race between
// "read tokens" and "write tokens").
//
// It uses Redis' own clock (TIME) instead of the app server's clock, so
// clock skew between your app servers does not affect the limit.
//
// KEYS[1] = bucket key
// ARGV[1] = rate (tokens/sec), ARGV[2] = burst, ARGV[3] = cost
// Returns {allowed, tokens_left, retry_after_sec, reset_after_sec}; floats are
// returned as strings because Redis truncates Lua numbers to integers.
var tokenBucketScript = redis.NewScript(`
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil then
  tokens = burst
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(burst, tokens + elapsed * rate)

local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = (cost - tokens) / rate
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
-- A full bucket is the same as no bucket, so let Redis delete idle keys.
redis.call('PEXPIRE', key, math.ceil((burst / rate) * 1000) + 1000)

local reset_after = (burst - tokens) / rate
return {allowed, tostring(tokens), tostring(retry_after), tostring(reset_after)}
`)

// Redis is a distributed token-bucket limiter: every app instance pointing at
// the same Redis shares one limit per key. One round trip per request.
type Redis struct {
	client redis.Scripter
	rule   Rule
	prefix string
}

// NewRedis builds a Redis-backed limiter. client can be *redis.Client,
// *redis.ClusterClient or a Sentinel failover client. prefix namespaces keys,
// e.g. "rl:api:" so different rules don't collide.
func NewRedis(client redis.Scripter, rule Rule, prefix string) (*Redis, error) {
	if err := rule.validate(); err != nil {
		return nil, err
	}
	return &Redis{client: client, rule: rule, prefix: prefix}, nil
}

// Allow implements Limiter. Always pass a context with a short timeout
// (e.g. 50ms) so a slow Redis cannot stall your API.
func (r *Redis) Allow(ctx context.Context, key string, cost int) (Result, error) {
	if cost <= 0 {
		cost = 1
	}
	vals, err := tokenBucketScript.Run(ctx, r.client,
		[]string{r.prefix + key},
		r.rule.Rate, r.rule.Burst, cost,
	).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("limiter: redis: %w", err)
	}
	return parseScriptReply(vals, r.rule.Burst)
}

// parseScriptReply decodes {allowed, remaining, retry_after_sec,
// reset_after_sec}, the reply shape shared by both Lua scripts.
func parseScriptReply(vals []any, limit int) (Result, error) {
	if len(vals) != 4 {
		return Result{}, fmt.Errorf("limiter: redis: unexpected reply %v", vals)
	}

	allowed, _ := vals[0].(int64)
	remaining, err1 := parseFloat(vals[1])
	retry, err2 := parseFloat(vals[2])
	reset, err3 := parseFloat(vals[3])
	if err1 != nil || err2 != nil || err3 != nil {
		return Result{}, fmt.Errorf("limiter: redis: bad reply %v", vals)
	}

	return Result{
		Allowed:    allowed == 1,
		Limit:      limit,
		Remaining:  int(math.Floor(remaining)),
		RetryAfter: secs(retry),
		ResetAfter: secs(reset),
	}, nil
}

func parseFloat(v any) (float64, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("not a string: %T", v)
	}
	return strconv.ParseFloat(s, 64)
}
