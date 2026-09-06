-- ack.lua 倒序版
local pending_key = 'pending:'..KEYS[1]
local min_offset_key = 'min_offset:'..KEYS[1]
local ack_offset = tonumber(ARGV[1]) or 0

local min_offset = tonumber(redis.call('GET', min_offset_key)) or 0

if ack_offset < min_offset then
    return min_offset
end

local items = redis.call('LPOP', pending_key, ack_offset - min_offset + 1)
return redis.call('INCRBY', min_offset_key, #items)