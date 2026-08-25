local pending_key = 'pending:'..KEYS[1]
local processing_key = 'processing:'..KEYS[1]
local count = tonumber(ARGV[1]) or 0

local items = redis.call('LPOP', pending_key, count)
if not items then
    return nil
end

for i = 1, #items do
    redis.call('RPUSH', processing_key, items[i])
end

return items