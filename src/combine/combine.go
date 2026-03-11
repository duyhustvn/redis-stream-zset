package combine

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
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/sonyflake"
)

// Cấu hình Redis & Constants
const (
	ChunkSize     = 15000
	RedisAddr     = "localhost:6379"
	AppPrefix     = "event"
	ActiveEvents  = AppPrefix + ":active_events"
	ChunkRegistry = AppPrefix + ":chunk_registry"
	PackLockKey   = AppPrefix + "lock:pack_chunk"
)

var (
	rdb        *redis.Client
	sf         *sonyflake.Sonyflake
	ctx        = context.Background()
	carriers   = []string{"Viettel", "Mobifone", "Vinaphone", "Vietnamobile"}
	categories = []string{"SCAM", "SPAM", "TELEMARKETING", "DEBT_COLLECTION"}
	actions    = []string{"ADD", "UPDATE", "DELETE"}
)

// Hàm random string
func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

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
}

// --- API 1: Generate Data ---
func generateHandler(w http.ResponseWriter, r *http.Request) {
	countStr := r.URL.Query().Get("count")
	count, err := strconv.Atoi(countStr)
	if err != nil || count <= 0 {
		http.Error(w, "Tham số count không hợp lệ", http.StatusBadRequest)
		return
	}

	for i := 0; i < count; i++ {
		sid, _ := sf.NextID()
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
			SID:         sid, // Dùng Sonyflake làm SID
		}

		eventJSON, _ := json.Marshal(logEntry)

		// Thêm vào Active Chunk, Score chính là SID
		rdb.ZAdd(ctx, ActiveEvents, redis.Z{
			Score:  float64(sid),
			Member: string(eventJSON),
		})
	}

	// Đóng gói nếu đủ 15.000 records
	go packChunkIfNeeded()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Đã tạo %d bản ghi EventLog", count)
}

func packChunkIfNeeded() {
	// 1. Thử lấy Lock từ Redis.
	// TTL = 30s để phòng trường hợp instance đang pack thì bị crash (OOM, sập nguồn),
	// Lock sẽ tự động nhả sau 30s để instance khác còn vào làm tiếp (chống Deadlock).
	locked, err := rdb.SetNX(ctx, PackLockKey, "1", 30*time.Second).Result()
	if err != nil || !locked {
		// Không lấy được lock (nghĩa là một instance khác đang pack rồi) -> Bỏ qua ngay
		log.Println("Instance khác đang pack chunk, skip...")
		return
	}

	// 2. Đảm bảo lúc nào xong việc cũng phải trả lại Lock cho người khác
	defer rdb.Del(ctx, PackLockKey)

	// 3. Bắt đầu tiến hành pack như bình thường
	for {
		count := rdb.ZCard(ctx, ActiveEvents).Val()
		if count < ChunkSize {
			break
		}

		// Lấy 15.000 bản ghi cũ nhất
		eventsStr, _ := rdb.ZRange(ctx, ActiveEvents, 0, ChunkSize-1).Result()

		var events []model.EventLog
		var maxSID uint64

		for _, evStr := range eventsStr {
			var ev model.EventLog
			json.Unmarshal([]byte(evStr), &ev)
			events = append(events, ev)
			if ev.SID > maxSID {
				maxSID = ev.SID
			}
		}

		// Tạo tên chunk và lưu chuỗi JSON
		chunkKey := fmt.Sprintf("%s:chunk_%d", AppPrefix, maxSID)
		chunkJSON, _ := json.Marshal(events)
		rdb.Set(ctx, chunkKey, chunkJSON, 0)
		// Xóa sau 7 ngày
		rdb.Set(ctx, chunkKey, chunkJSON, 7*24*time.Hour)

		// Ghi vào Sổ đăng ký
		rdb.ZAdd(ctx, ChunkRegistry, redis.Z{
			Score:  float64(maxSID),
			Member: chunkKey,
		})

		// Xoá an toàn theo SID
		rdb.ZRemRangeByScore(ctx, ActiveEvents, "-inf", fmt.Sprintf("%d", maxSID))

		log.Printf("Đã đóng gói thành công: %s", chunkKey)
	}
}

// --- API 2: Lấy dữ liệu đồng bộ ---
func syncHandler(w http.ResponseWriter, r *http.Request) {
	lastIDStr := r.URL.Query().Get("last_id")
	lastID, err := strconv.ParseUint(lastIDStr, 10, 64)
	if err != nil {
		lastID = 0
	}

	w.Header().Set("Content-Type", "application/json")
	minScore := fmt.Sprintf("(%d", lastID) // Lớn hơn lastID

	// Tìm trong Registry
	chunks, _ := rdb.ZRangeByScore(ctx, ChunkRegistry, &redis.ZRangeBy{
		Min: minScore, Max: "+inf", Offset: 0, Count: 1,
	}).Result()

	// Trạng thái 1: Lấy từ Chunk tĩnh
	if len(chunks) > 0 {
		chunkData, _ := rdb.Get(ctx, chunks[0]).Result()
		var allEvents []model.EventLog
		json.Unmarshal([]byte(chunkData), &allEvents)

		var filtered []model.EventLog
		for _, ev := range allEvents {
			if ev.SID > lastID {
				filtered = append(filtered, ev)
			}
		}
		json.NewEncoder(w).Encode(filtered)
		return
	}

	// Trạng thái 2: Lấy từ Active Events
	activeEventsStr, _ := rdb.ZRangeByScore(ctx, ActiveEvents, &redis.ZRangeBy{
		Min: minScore, Max: "+inf", Offset: 0, Count: ChunkSize,
	}).Result()

	var active []model.EventLog
	for _, evStr := range activeEventsStr {
		var ev model.EventLog
		json.Unmarshal([]byte(evStr), &ev)
		active = append(active, ev)
	}
	json.NewEncoder(w).Encode(active)
}

func Routes() {
	http.HandleFunc("/api/generate", generateHandler)
	http.HandleFunc("/api/sync", syncHandler)

	fmt.Println("Server đang chạy tại http://localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
