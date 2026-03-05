package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/sonyflake"
)

const (
	StreamName   = "spam_call_wall_log"
	MaxStreamLen = 1000000
)

var (
	ctx = context.Background()
	rdb *redis.Client
	sf  *sonyflake.Sonyflake
)

type Meta struct {
	SubCategory string `json:"subCategory"`
}

type PhoneNumber struct {
	Value         string `json:"value"`
	Carrier       string `json:"carrier"`
	Category      string `json:"category"`
	RiskLevel     int    `json:"risk_level"`
	Meta          Meta   `json:"meta"`
	UserRiskScore int    `json:"user_risk_score"`
}

type EventLog struct {
	ID          string      `json:"id"`
	PhoneNumber PhoneNumber `json:"phoneNumber"`
	Type        string      `json:"type"`
	CreatedTime int64       `json:"createdTime"`
	SID         uint64      `json:"sid"`
}

func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "172.17.0.1:6379",
		Password: "changeme",
	})

	var st sonyflake.Settings
	// Khởi tạo một lần và không bao giờ thay đổi mốc này ở các lần restart server.
	st.StartTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	sf = sonyflake.NewSonyflake(st)
	if sf == nil {
		log.Fatal("Không thể khởi tạo Sonyflake")
	}
}

// --- Hàm Helper: Tạo chuỗi random cho trường "id" (giống format ES/Firestore) ---
func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
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
		logEntry := EventLog{
			ID: randomString(20), // VD: 6QawtpwB-xMT8VeR6JaW
			PhoneNumber: PhoneNumber{
				Value:         fmt.Sprintf("+84%d", 900000000+rand.Intn(99999999)),
				Carrier:       carriers[rand.Intn(len(carriers))],
				Category:      categories[rand.Intn(len(categories))],
				RiskLevel:     100,
				Meta:          Meta{SubCategory: "spam"},
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

func handleInit(w http.ResponseWriter, r *http.Request) {
	count := 150000
	if c := r.URL.Query().Get("count"); c != "" {
		count, _ = strconv.Atoi(c)
	}

	rdb.Del(ctx, StreamName) // Clear data cũ
	lastID, err := generateData(count)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": fmt.Sprintf("Đã khởi tạo thành công %d bản ghi", count),
		"last_id": lastID,
	})
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

	limit := 1000
	if l := r.URL.Query().Get("limit"); l != "" {
		limit, _ = strconv.Atoi(l)
	}

	streamInfo, err := rdb.XInfoStream(ctx, StreamName).Result()
	if err != nil && err != redis.Nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if streamInfo != nil && len(streamInfo.FirstEntry.ID) > 0 {
		oldestRedisID := streamInfo.FirstEntry.ID
		parts := strings.Split(oldestRedisID, "-")
		oldestSFID, _ := strconv.ParseUint(parts[0], 10, 64)

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

	clientRedisID := fmt.Sprintf("%d-0", clientLastSFID)
	streams, err := rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{StreamName, clientRedisID},
		Count:   int64(limit),
		Block:   0,
	}).Result()

	if err == redis.Nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "up_to_date",
			"events": []EventLog{}, // Trả về mảng rỗng
		})
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var events []EventLog
	var newLastID uint64

	for _, msg := range streams[0].Messages {
		parts := strings.Split(msg.ID, "-")
		sfid, _ := strconv.ParseUint(parts[0], 10, 64)
		newLastID = sfid

		var logEntry EventLog
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

func main() {
	http.HandleFunc("/api/init", handleInit)
	http.HandleFunc("/api/generate", handleGenerate)
	http.HandleFunc("/api/sync", handleSync)
	http.HandleFunc("/api/stats", handleStats)

	fmt.Println("Server run at http://localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
