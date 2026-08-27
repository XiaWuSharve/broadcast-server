-- push.lua
local pending_key = 'pending:'..KEYS[1]
local max_offset_key = 'max_offset:'..KEYS[1]
redis.call('RPUSH', pending_key, ARGV[1])
return redis.call('INCR', max_offset_key)