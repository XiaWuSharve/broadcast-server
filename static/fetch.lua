-- fetch.lua
local pending_key = 'pending:'..KEYS[1]
local min_offset_key = 'min_offset:'..KEYS[1]
local request_offset = tonumber(ARGV[1]) or 0

local min_offset = tonumber(redis.call('GET', min_offset_key)) or 0
if request_offset < min_offset then
    return nil
end
local items = redis.call('LRANGE', pending_key, 0, request_offset - min_offset)

return items