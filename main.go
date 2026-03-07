package main

import (
	"log"
	"redis-stream-demo/src/config"
	redisclient "redis-stream-demo/src/pkg/redis"
	"redis-stream-demo/src/zset"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Cannot load env: %+v", err)
	}

	_, err = redisclient.NewRedisClient(cfg.RedisConfig)
	if err != nil {
		log.Fatalf("Failed to init redis client: %+v", err)
	}

	zset.Routes()
}
