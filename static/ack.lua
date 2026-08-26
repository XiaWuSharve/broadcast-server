local processing_key = 'processing:'..KEYS[1]
local count = tonumber(ARGV[1]) or 0

local items = redis.call('LPOP', processing_key, count)
if not items then
    return nil
end

return #items