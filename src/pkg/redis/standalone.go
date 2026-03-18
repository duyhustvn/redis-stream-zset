package redisclient

import (
	"context"
	"log"
	"redis-stream-demo/src/config"
	"time"

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

		// 1. Mở rộng Pool
		PoolSize:     200,
		MinIdleConns: 100,

		// 2. ReadTimeout thấp để fail-fast, tránh ghost connections chất đống
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,

		// 3. Thời gian tối đa một request chịu chờ để lấy connection từ Pool
		PoolTimeout: 6 * time.Second,
		DialTimeout: 3 * time.Second,
	})

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	log.Println("Connected redis standalone successfully:", pong)
	return rdb, nil
}
