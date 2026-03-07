package redisclient

import (
	"fmt"
	"redis-stream-demo/src/config"

	"github.com/redis/go-redis/v9"
)

func NewRedisClient(redisCfg config.RedisConfig) (*redis.Client, error) {
	switch redisCfg.Mode {
	case "standalone":
		return NewRedisStandaloneClient(redisCfg)
	case "sentinel":
		return NewRedisSentinelClient(redisCfg)
	default:
		return nil, fmt.Errorf("Not support redis mode: %s", redisCfg.Mode)
	}
}
