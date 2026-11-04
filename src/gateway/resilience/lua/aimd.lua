-- AIMD on a provider's effective outbound rate, shared by every replica.
--
-- KEYS[1]  aimd hash {eff, last_dec, last_inc, blocked_until}
-- ARGV[1]  mode: 'decrease' (we saw a 429) | 'tick' (periodic refresh)
-- ARGV[2]  max_rps (configured)    ARGV[3] min_rps
-- ARGV[4]  decrease factor         ARGV[5] additive step (rps)
-- ARGV[6]  window_ms               ARGV[7] retry_after_ms from the provider (decrease only)
-- returns  {effective_rps (string, keeps decimals), blocked_remaining_ms}
--
-- One halving per window, however many replicas saw a 429 in it; one increase per
-- clean window (no 429 for a full window), however many replicas tick.

local mode = ARGV[1]
local max_rps = tonumber(ARGV[2])
local min_rps = tonumber(ARGV[3])
local factor = tonumber(ARGV[4])
local step = tonumber(ARGV[5])
local window = tonumber(ARGV[6])
local retry_after = tonumber(ARGV[7]) or 0

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local h = redis.call('HMGET', KEYS[1], 'eff', 'last_dec', 'last_inc', 'blocked_until')
local eff = tonumber(h[1]) or max_rps
local last_dec = tonumber(h[2]) or 0
local last_inc = tonumber(h[3]) or 0
local blocked = tonumber(h[4]) or 0
eff = math.max(min_rps, math.min(eff, max_rps))

if mode == 'decrease' then
  if now - last_dec >= window then
    eff = math.max(min_rps, eff * factor)
    last_dec = now
    last_inc = now
  end
  if retry_after > 0 then
    blocked = math.max(blocked, now + retry_after)
  end
elseif mode == 'tick' then
  if now - last_dec >= window and now - last_inc >= window then
    eff = math.min(max_rps, eff + step)
    last_inc = now
  end
end

redis.call('HSET', KEYS[1], 'eff', eff, 'last_dec', last_dec, 'last_inc', last_inc,
           'blocked_until', blocked)
redis.call('PEXPIRE', KEYS[1], 3600000)
return {tostring(eff), math.max(0, blocked - now)}
