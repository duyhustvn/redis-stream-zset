package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"redis-stream-demo/src/config"
	"redis-stream-demo/src/model"
	redisclient "redis-stream-demo/src/pkg/redis"
	"redis-stream-demo/src/pkg/util"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/sonyflake"
)

const (
	StreamName   = "stream"
	MaxStreamLen = 1000000
)

var (
	ctx = context.Background()
	rdb *redis.Client
	sf  *sonyflake.Sonyflake
)

func init() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Cannot load env: %+v", err)
	}

	rdb, err = redisclient.NewRedisClient(cfg.RedisConfig)
	if err != nil {
		log.Fatalf("Failed to init redis client: %+v", err)
	}

	var st sonyflake.Settings
	// Khởi tạo một lần và không bao giờ thay đổi mốc này ở các lần restart server.
	st.StartTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	sf = sonyflake.NewSonyflake(st)
	if sf == nil {
		log.Fatal("Không thể khởi tạo Sonyflake")
	}
}

// --- Hàm Helper: Sinh data mock sát với thực tế ---
func generateData(count int) (uint64, error) {
	pipe := rdb.Pipeline()
	var lastSFID uint64

	carriers := []string{"Viettel", "Mobifone", "Vinaphone", "Việt Nam"}
	categories := []string{"scam", "spam", "suspectSpam", "suspectScam"}
	actions := []string{"update", "add"}

	for i := 1; i <= count; i++ {
		id, _ := sf.NextID()
		lastSFID = id
		redisID := fmt.Sprintf("%d-0", id)

		// Tạo Mock Object khớp với JSON mẫu
		logEntry := model.EventLog{
			ID: util.RandomString(20), // randomString(20), // VD: 6QawtpwB-xMT8VeR6JaW
			PhoneNumber: model.PhoneNumber{
				Value:         fmt.Sprintf("+84%d", 900000000+rand.Intn(99999999)),
				Carrier:       carriers[rand.Intn(len(carriers))],
				Category:      categories[rand.Intn(len(categories))],
				RiskLevel:     100,
				Meta:          model.Meta{SubCategory: "spam"},
				UserRiskScore: rand.Intn(100),
			},
			Type:        actions[rand.Intn(len(actions))],
			CreatedTime: time.Now().Unix(),
			SID:         id, // Gán Sonyflake ID vào trường sid
		}

		// Convert Object thành JSON String để lưu vào Redis
		jsonBytes, _ := json.Marshal(logEntry)
		jsonString := string(jsonBytes)

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: StreamName,
			MaxLen: MaxStreamLen,
			Approx: true,
			ID:     redisID,
			Values: map[string]interface{}{
				"action": logEntry.Type, // Để parse nhanh ở mức Stream
				"data":   jsonString,    // Chứa toàn bộ object JSON
			},
		})

		// Commit mỗi 5000 bản ghi để tối ưu RAM
		if i%5000 == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return 0, err
			}
			fmt.Printf("Đã sinh %d bản ghi...\n", i)
		}
	}

	if count%5000 != 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, err
		}
	}

	return lastSFID, nil
}

func handleGenerate(w http.ResponseWriter, r *http.Request) {
	count := 10
	if c := r.URL.Query().Get("count"); c != "" {
		count, _ = strconv.Atoi(c)
	}

	lastID, err := generateData(count)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": fmt.Sprintf("Đã chèn thêm %d log thay đổi trạng thái thuê bao", count),
		"last_id": lastID,
	})
}

func handleSync(w http.ResponseWriter, r *http.Request) {
	apiStartTime := time.Now()

	w.Header().Set("Content-Type", "application/json")
	lastIDStr := r.URL.Query().Get("last_id")
	if lastIDStr == "" {
		http.Error(w, `{"error": "Thiếu tham số last_id"}`, http.StatusBadRequest)
		return
	}

	clientLastSFID, err := strconv.ParseUint(lastIDStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error": "last_id không hợp lệ"}`, http.StatusBadRequest)
		return
	}

	limit := 15000
	if l := r.URL.Query().Get("limit"); l != "" {
		limit, _ = strconv.Atoi(l)
	}

	var redisQueryTime time.Duration

	redisStartTime := time.Now()

	streamInfo, err := rdb.XInfoStream(ctx, StreamName).Result()
	redisQueryTime += time.Since(redisStartTime)
	if err != nil && err != redis.Nil {
		log.Printf("Query Stream Redis Info failed: %+v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if streamInfo != nil && len(streamInfo.FirstEntry.ID) > 0 {
		oldestRedisID := streamInfo.FirstEntry.ID
		parts := strings.Split(oldestRedisID, "-")
		oldestSFID, _ := strconv.ParseUint(parts[0], 10, 64)

		// Kiểm tra Out of Sync (Lấy bản ghi cũ nhất trong Stream)
		if clientLastSFID > 0 && clientLastSFID < oldestSFID {
			w.WriteHeader(http.StatusGone) // HTTP 410
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "out_of_sync",
				"message": "Dữ liệu quá cũ, hãy tải file snapshot mới nhất",
				"action":  "full_reload",
			})
			return
		}
	}

	redisStartTime = time.Now()
	clientRedisID := fmt.Sprintf("%d-0", clientLastSFID)
	streams, err := rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{StreamName, clientRedisID},
		Count:   int64(limit),
		Block:   -1, //  Không block đợi dữ liệu nếu dữ liệu đã là mới nhất
	}).Result()
	redisQueryTime += time.Since(redisStartTime)

	if err == redis.Nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "up_to_date",
			"events": []model.EventLog{}, // Trả về mảng rỗng
		})
		return
	} else if err != nil {
		log.Printf("Query Stream Redis failed: %+v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var events []model.EventLog
	var newLastID uint64

	for _, msg := range streams[0].Messages {
		parts := strings.Split(msg.ID, "-")
		sfid, _ := strconv.ParseUint(parts[0], 10, 64)
		newLastID = sfid

		var logEntry model.EventLog
		dataStr := msg.Values["data"].(string)
		json.Unmarshal([]byte(dataStr), &logEntry)

		events = append(events, logEntry)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "success",
		"count_returned": len(events),
		"next_last_id":   newLastID,
		"events":         events,
	})
	apiDuration := time.Since(apiStartTime)
	log.Printf("[SYNC API] last_id: %d | Records: %d | Redis Time: %v | Total API Time: %v",
		clientLastSFID, len(events), redisQueryTime, apiDuration)
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Lấy tổng số lượng bản ghi đang có trong Stream
	length, err := rdb.XLen(ctx, StreamName).Result()
	if err != nil && err != redis.Nil {
		http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	// Đo lường chính xác dung lượng RAM (tính bằng Bytes) của Stream này
	memUsageBytes, err := rdb.MemoryUsage(ctx, StreamName).Result()
	if err == redis.Nil {
		memUsageBytes = 0 // Key chưa tồn tại (Stream trống)
	} else if err != nil {
		http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	kb := float64(memUsageBytes) / 1024
	mb := float64(memUsageBytes) / (1024 * 1024)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"stream_name":   StreamName,
		"total_records": length,
		"memory_usage": map[string]interface{}{
			"bytes": memUsageBytes,
			"kb":    fmt.Sprintf("%.2f KB", kb),
			"mb":    fmt.Sprintf("%.2f MB", mb),
		},
		"average_size_per_record": map[string]interface{}{
			"bytes": func() float64 {
				if length == 0 {
					return 0
				}
				return float64(memUsageBytes) / float64(length)
			}(),
		},
	})
}

// --- API 5: Cấp phát ID hợp lệ cho k6 Load Test (Phiên bản Stream) ---
// Method: GET /api/test/setup
func handleTestSetup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Lấy 500 bản ghi cũ nhất nằm ở đầu Stream
	// Dấu "-" đại diện cho ID nhỏ nhất, dấu "+" đại diện cho ID lớn nhất
	messages, err := rdb.XRangeN(ctx, StreamName, "-", "+", 500).Result()
	if err != nil && err != redis.Nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var validIDs []uint64
	for _, msg := range messages {
		// Trong Stream, ID được lưu dưới dạng "SonyflakeID-0" (VD: "116427239398554511-0")
		// Ta tách chuỗi theo dấu "-" để lấy ID thực tế
		parts := strings.Split(msg.ID, "-")
		if len(parts) > 0 {
			sfid, _ := strconv.ParseUint(parts[0], 10, 64)
			validIDs = append(validIDs, sfid)
		}
	}

	// Fallback an toàn phòng khi Stream đang trống
	if len(validIDs) == 0 {
		validIDs = append(validIDs, 0)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"sids": validIDs,
	})
}

func Routes() {
	http.HandleFunc("/api/generate", handleGenerate)
	http.HandleFunc("/api/sync", handleSync)
	http.HandleFunc("/api/stats", handleStats)
	http.HandleFunc("/api/test/setup", handleTestSetup)

	fmt.Println("Server run at http://localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
