package zset

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
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/sonyflake"
)

const (
	ZSetName       = "zset"
	MaxZSetLen     = 1000000
	WatermarkDelay = 5 * time.Second // Độ trễ 5 giây để đợi các Worker chậm chạp
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
	// FIX CỨNG mốc thời gian để chống lỗi đồng hồ chạy lùi khi restart
	st.StartTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	sf = sonyflake.NewSonyflake(st)
	if sf == nil {
		log.Fatal("Không thể khởi tạo Sonyflake")
	}

	// Chạy 1 Background Job để dọn rác ZSET (Giữ đúng 1 triệu bản ghi mới nhất)
	go func() {
		for {
			time.Sleep(1 * time.Minute)
			// Xóa các bản ghi cũ, chỉ giữ lại MaxZSetLen bản ghi có rank cao nhất
			rdb.ZRemRangeByRank(ctx, ZSetName, 0, -(MaxZSetLen + 1))
		}
	}()
}

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

// --- Hàm Helper sinh data ---
func generateData(count int) (uint64, error) {
	pipe := rdb.Pipeline()
	var lastSFID uint64

	carriers := []string{"Viettel", "Mobifone", "Vinaphone", "Việt Nam"}
	categories := []string{"scam", "spam", "suspectSpam"}
	actions := []string{"update", "add"}

	for i := 1; i <= count; i++ {
		id, _ := sf.NextID()
		lastSFID = id

		logEntry := model.EventLog{
			ID: randomString(20),
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
			SID:         id,
		}

		jsonBytes, _ := json.Marshal(logEntry)

		// KỸ THUẬT LEXICOGRAPHICAL: Format SID thành chuỗi 20 ký tự số (Padding 0 ở đầu)
		// Ví dụ: "000016777216327946:{"id":"..."}"
		memberStr := fmt.Sprintf("%020d:%s", id, string(jsonBytes))

		pipe.ZAdd(ctx, ZSetName, redis.Z{
			Score:  0, // Tất cả score = 0, ép Redis sắp xếp theo chữ cái của memberStr
			Member: memberStr,
		})

		if i%5000 == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return 0, err
			}
			fmt.Printf("Đã sinh %d bản ghi vào ZSET...\n", i)
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
	rdb.Del(ctx, ZSetName)
	lastID, err := generateData(count)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"message": "Init success", "last_id": lastID})
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
	json.NewEncoder(w).Encode(map[string]interface{}{"message": "Generate success", "last_id": lastID})
}

func handleSync(w http.ResponseWriter, r *http.Request) {
	apiStartTime := time.Now()
	w.Header().Set("Content-Type", "application/json")
	lastIDStr := r.URL.Query().Get("last_id")

	clientLastSFID, err := strconv.ParseUint(lastIDStr, 10, 64)
	if err != nil || clientLastSFID == 0 {
		http.Error(w, `{"error": "last_id không hợp lệ"}`, http.StatusBadRequest)
		return
	}

	limit := 10000
	if l := r.URL.Query().Get("limit"); l != "" {
		limit, _ = strconv.Atoi(l)
	}

	// Kiểm tra Out of Sync (Lấy bản ghi cũ nhất trong ZSET)
	oldestRecords, err := rdb.ZRange(ctx, ZSetName, 0, 0).Result()
	if err == nil && len(oldestRecords) > 0 {
		// Bóc tách SID từ chuỗi member "000016777216327946:{"id..."}"
		parts := strings.SplitN(oldestRecords[0], ":", 2)
		oldestSID, _ := strconv.ParseUint(parts[0], 10, 64)

		if clientLastSFID < oldestSID {
			w.WriteHeader(http.StatusGone) // HTTP 410
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "out_of_sync",
				"message": "Dữ liệu quá cũ, hãy tải file snapshot",
			})
			return
		}
	}

	// Thực hiện Range Query theo từ điển (Lexicographical) bằng ZRangeArgs
	minLex := fmt.Sprintf("(%020d:", clientLastSFID) // '(' nghĩa là Lớn hơn
	maxLex := "+"                                    // '+' nghĩa là phần tử lớn nhất

	members, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:    ZSetName,
		ByLex:  true, // Bật chế độ quét theo Lexicographical (bảng chữ cái)
		Start:  minLex,
		Stop:   maxLex,
		Offset: 0,            // Bắt đầu lấy từ phần tử đầu tiên thỏa mãn
		Count:  int64(limit), // Giới hạn số lượng trả về (LIMIT)
	}).Result()

	if err != nil && err != redis.Nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 4. Parse dữ liệu trả về
	var events []model.EventLog
	var newLastID uint64 = clientLastSFID

	for _, member := range members {
		parts := strings.SplitN(member, ":", 2)
		if len(parts) == 2 {
			sfid, _ := strconv.ParseUint(parts[0], 10, 64)
			newLastID = sfid

			var logEntry model.EventLog
			json.Unmarshal([]byte(parts[1]), &logEntry)
			events = append(events, logEntry)
		}
	}

	elapsed := time.Since(apiStartTime).Milliseconds()
	fmt.Printf("Elapsed Time: %d mili \n", elapsed)

	// Nếu không có data nào trong khoảng (last_id -> safe_id)
	if len(events) == 0 {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "up_to_date",
			"events": []model.EventLog{},
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "success",
		"count_returned": len(events),
		"next_last_id":   newLastID,
		"events":         events,
	})
}

// --- API 4: Đo dung lượng RAM ---
func handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	length, _ := rdb.ZCard(ctx, ZSetName).Result()
	memUsageBytes, _ := rdb.MemoryUsage(ctx, ZSetName).Result()

	mb := float64(memUsageBytes) / (1024 * 1024)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"structure":     "ZSET (Lexicographical)",
		"total_records": length,
		"memory_mb":     fmt.Sprintf("%.2f MB", mb),
	})
}

// --- API 5: Cấp phát ID hợp lệ cho k6 Load Test ---
// Method: GET /api/test/setup
func handleTestSetup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Lấy 500 bản ghi cũ nhất (nằm ở đáy ZSET)
	members, err := rdb.ZRange(ctx, ZSetName, 0, 500).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var validIDs []uint64
	for _, member := range members {
		parts := strings.SplitN(member, ":", 2)
		sfid, _ := strconv.ParseUint(parts[0], 10, 64)
		validIDs = append(validIDs, sfid)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"sids": validIDs,
	})
}

func Routes() {
	http.HandleFunc("/api/init", handleInit)
	http.HandleFunc("/api/generate", handleGenerate)
	http.HandleFunc("/api/sync", handleSync)
	http.HandleFunc("/api/stats", handleStats)
	http.HandleFunc("/api/test/setup", handleTestSetup)

	fmt.Println("Server chạy tại http://localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
