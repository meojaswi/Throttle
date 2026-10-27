package limiter

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// slidingWindowScript is windowDecision (window.go) running atomically inside
// Redis. Like the token bucket script it uses Redis' own clock (TIME) and a
// single hash key per client, so it is race-free across app instances and
// safe on Redis Cluster.
//
// KEYS[1] = counter key
// ARGV[1] = window (seconds, may be fractional), ARGV[2] = limit, ARGV[3] = cost
// Returns {allowed, remaining, retry_after_sec, reset_after_sec}; floats are
// returned as strings because Redis truncates Lua numbers to integers.
var slidingWindowScript = redis.NewScript(`
local key   = KEYS[1]
local w     = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local idx = math.floor(now / w)
local elapsed = now - idx * w

local state = redis.call('HMGET', key, 'idx', 'curr', 'prev')
local sidx = tonumber(state[1])
local curr = tonumber(state[2]) or 0
local prev = tonumber(state[3]) or 0
if sidx == nil or sidx < idx - 1 then
  prev = 0
  curr = 0
elseif sidx == idx - 1 then
  prev = curr
  curr = 0
end
if sidx ~= nil and sidx > idx then
  idx = sidx
end

local prev_weighted = prev * (1 - elapsed / w)
local est = prev_weighted + curr

local allowed = 0
local retry = 0
if est + cost <= limit then
  allowed = 1
  curr = curr + cost
  est = est + cost
else
  local need = est + cost - limit
  local tail = w - elapsed
  if prev > 0 and prev_weighted >= need then
    retry = need * w / prev
  elseif curr + cost - limit <= curr and curr > 0 then
    retry = tail + (curr + cost - limit) * w / curr
  else
    retry = tail + w
  end
end

redis.call('HSET', key, 'idx', idx, 'curr', curr, 'prev', prev)
-- After two windows both counters have slid out; let Redis drop the key.
redis.call('PEXPIRE', key, math.ceil(w * 2000) + 1000)

local remaining = math.max(0, limit - est)
local reset = 0
if curr > 0 then
  reset = (w - elapsed) + w
elseif prev_weighted > 0 then
  reset = w - elapsed
end
return {allowed, tostring(remaining), tostring(retry), tostring(reset)}
`)

// RedisWindow is a distributed sliding window counter limiter: every app
// instance pointing at the same Redis shares one quota per key.
type RedisWindow struct {
	client redis.Scripter
	rule   WindowRule
	prefix string
}

// NewRedisWindow builds a Redis-backed sliding window limiter. Use a prefix
// different from your token-bucket limiters (e.g. "rl:otp:") since the two
// store different fields under their keys.
func NewRedisWindow(client redis.Scripter, rule WindowRule, prefix string) (*RedisWindow, error) {
	if err := rule.validate(); err != nil {
		return nil, err
	}
	return &RedisWindow{client: client, rule: rule, prefix: prefix}, nil
}

// Allow implements Limiter. Pass a context with a short timeout.
func (r *RedisWindow) Allow(ctx context.Context, key string, cost int) (Result, error) {
	if cost <= 0 {
		cost = 1
	}
	vals, err := slidingWindowScript.Run(ctx, r.client,
		[]string{r.prefix + key},
		r.rule.Window.Seconds(), r.rule.Limit, cost,
	).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("limiter: redis: %w", err)
	}
	return parseScriptReply(vals, r.rule.Limit)
}
