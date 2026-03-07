package redisclient

import (
	"context"
	"log"
	"redis-stream-demo/src/config"

	"github.com/redis/go-redis/v9"
)

func NewRedisStandaloneClient(redisCfg config.RedisConfig) (*redis.Client, error) {
	// A context is used for cancellation and timeouts
	ctx := context.Background()

	// Connect to the Redis server
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisCfg.Address,
		Password: redisCfg.Password,
		DB:       0,
	})

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	log.Println("Connected redis standalone successfully:", pong)
	return rdb, nil
}
