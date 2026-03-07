package redisclient

import (
	"context"
	"log"
	"redis-stream-demo/src/config"
	"strings"

	"github.com/redis/go-redis/v9"
)

func NewRedisSentinelClient(redisCfg config.RedisConfig) (*redis.Client, error) {
	ctx := context.Background()

	sentinelAddrs := strings.Split(redisCfg.Address, ",")

	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    redisCfg.MasterName,
		SentinelAddrs: sentinelAddrs,
		Password:      redisCfg.Password,
	})

	// Test the connection
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	log.Println("Connected successfully:", pong)

	return rdb, nil
}
