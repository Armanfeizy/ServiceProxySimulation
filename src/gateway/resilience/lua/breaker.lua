-- Circuit breaker state machine, shared by every replica.
-- Mirrors gateway.resilience.breaker.LocalBreaker; keep the two in sync.
--
-- KEYS[1]  state hash   {state, opened_at}
-- KEYS[2]  counts hash  {"<epoch_s>:t" -> total, "<epoch_s>:f" -> failures}
-- KEYS[3]  probe lock   (only one replica probes a HALF_OPEN provider)
-- ARGV[1]  mode: 'tick' | 'probe_ok' | 'probe_fail'
-- ARGV[2]  window_s   ARGV[3] failure_ratio   ARGV[4] min_volume   ARGV[5] cooldown_ms
-- returns  {state, window_total, window_failures}

local mode = ARGV[1]
local window = tonumber(ARGV[2])
local ratio = tonumber(ARGV[3])
local min_volume = tonumber(ARGV[4])
local cooldown = tonumber(ARGV[5])

local t = redis.call('TIME')
local now_s = tonumber(t[1])
local now = now_s * 1000 + math.floor(tonumber(t[2]) / 1000)

local h = redis.call('HMGET', KEYS[1], 'state', 'opened_at')
local state = h[1] or 'closed'
local opened_at = tonumber(h[2]) or 0

local function open()
  state = 'open'
  opened_at = now
  redis.call('DEL', KEYS[2], KEYS[3])
end

local total, failures = 0, 0

if mode == 'tick' then
  local fields = redis.call('HGETALL', KEYS[2])
  local expired = {}
  for i = 1, #fields, 2 do
    local sec, kind = string.match(fields[i], '^(%d+):(%a)$')
    sec = tonumber(sec)
    if sec == nil or sec <= now_s - window then
      expired[#expired + 1] = fields[i]
    elseif kind == 't' then
      total = total + tonumber(fields[i + 1])
    else
      failures = failures + tonumber(fields[i + 1])
    end
  end
  if #expired > 0 then
    redis.call('HDEL', KEYS[2], unpack(expired))
  end
  if state == 'closed' then
    if total >= min_volume and failures / total > ratio then
      open()
    end
  elseif state == 'open' and now - opened_at >= cooldown then
    state = 'half_open'
  end
elseif mode == 'probe_ok' then
  if state ~= 'closed' then
    state = 'closed'
    redis.call('DEL', KEYS[2], KEYS[3])
  end
elseif mode == 'probe_fail' then
  open()
end

redis.call('HSET', KEYS[1], 'state', state, 'opened_at', opened_at)
redis.call('PEXPIRE', KEYS[1], 3600000)
return {state, total, failures}
