package main

import (
	"encoding/json"
	"log"
	"net/http"
	chunk "redis-stream-demo/src/chunk"
	"redis-stream-demo/src/config"
	redisclient "redis-stream-demo/src/pkg/redis"
	"redis-stream-demo/src/pkg/util"
	"redis-stream-demo/src/stream"
	"redis-stream-demo/src/zset"
	"strconv"
	"time"

	"github.com/sony/sonyflake"
)

func generateHandler(w http.ResponseWriter, r *http.Request) {
	countStr := r.URL.Query().Get("count")
	count, err := strconv.Atoi(countStr)
	if err != nil || count <= 0 {
		http.Error(w, "Tham số count không hợp lệ", http.StatusBadRequest)
		return
	}

	bucketStr := r.URL.Query().Get("bucket")
	bucket, err := strconv.Atoi(bucketStr)
	if err != nil || bucket <= 0 {
		http.Error(w, "Tham số bucket không hợp lệ", http.StatusBadRequest)
		return
	}

	var st sonyflake.Settings
	// FIX CỨNG mốc thời gian để chống lỗi đồng hồ chạy lùi khi restart
	st.StartTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	sf := sonyflake.NewSonyflake(st)

	logsEntry := util.GenerateData(sf, count)

	type sidBucket struct {
		Sid    uint64 `json:"sid"`
		Bucket uint64 `json:"bucket"`
	}

	sidBuckets := []sidBucket{}
	for _, logEntry := range logsEntry {
		sidBuckets = append(sidBuckets, sidBucket{
			Sid:    logEntry.SID,
			Bucket: (logEntry.SID / uint64(bucket)) * uint64(bucket),
		})
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"sids":      sidBuckets,
		"createdAt": time.Now().Format("2006-01-02 15:04:05"),
	})
}

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

	http.HandleFunc("/api/sids", generateHandler)

	switch cfg.Server.Mode {
	case "zset":
		zset.Routes()
	case "stream":
		stream.Routes()
	default:
		chunk.Routes()
	}

}
