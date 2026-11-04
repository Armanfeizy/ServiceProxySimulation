-- Atomic token bucket, shared by every replica.
--
-- KEYS[1]  bucket hash {tokens, ts}
-- ARGV[1]  refill rate, tokens/second (float)
-- ARGV[2]  burst capacity
-- ARGV[3]  cost of this call
-- returns  {allowed 0|1, retry_after_ms}
--
-- Time comes from the Redis server, never from callers, so replica clock skew
-- cannot inflate or starve the bucket.

local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end

local elapsed = now - ts
if elapsed > 0 then
  tokens = math.min(burst, tokens + elapsed * rate / 1000.0)
end

local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = math.ceil((cost - tokens) * 1000.0 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst * 1000.0 / rate) + 1000)
return {allowed, retry_after}
