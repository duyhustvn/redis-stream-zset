package main

import (
	"log"
	chunk "redis-stream-demo/src/chunk"
	"redis-stream-demo/src/config"
	redisclient "redis-stream-demo/src/pkg/redis"
	"redis-stream-demo/src/stream"
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
	log.Println("Running in mode: ", cfg.Server.Mode)

	switch cfg.Server.Mode {
	case "zset":
		zset.Routes()
	case "stream":
		stream.Routes()
	default:
		chunk.Routes()
	}
}
