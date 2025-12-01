package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Lua script for monotonic nonce / counter:
//
// KEYS[1] = key ("nonce:{device_id}")
// ARGV[1] = incoming value (string, integer)
//
// if existing != nil and incoming <= existing -> return 0 (reject)
// else set to incoming and return 1 (accept)
var monotonicScript = redis.NewScript(`
local last = redis.call("GET", KEYS[1])
if last ~= false and tonumber(ARGV[1]) <= tonumber(last) then
  return 0
end
redis.call("SET", KEYS[1], ARGV[1])
return 1
`)

// EnsureMonotonic ensures "value" is strictly greater than the stored value.
// Returns true if accepted and stored, false if rejected (replay / old value).
func EnsureMonotonic(ctx context.Context, r redis.Scripter, key string, value int64) (bool, error) {
	res, err := monotonicScript.Run(ctx, r, []string{key}, value).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
