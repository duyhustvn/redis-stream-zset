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
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/sonyflake"
)

// Cấu hình Redis & Constants
const (
	ChunkSize     = 15000
	AppPrefix     = "event"
	ActiveEvents  = AppPrefix + ":active_events"
	ChunkRegistry = AppPrefix + ":chunk_registry"
	PackLockKey   = AppPrefix + "lock:pack_chunk"
)

var (
	rdb        *redis.Client
	sf         *sonyflake.Sonyflake
	ctx        = context.Background()
	packSignal = make(chan struct{}, 1) // Channel kích hoạt worker
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

func Routes() {
	go chunkPackerWorker()

	http.HandleFunc("/api/generate", generateHandler)
	http.HandleFunc("/api/sync", syncHandler)

	fmt.Println("Server đang chạy tại http://localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
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
			SID:         sid,
		}

		eventJSON, _ := json.Marshal(logEntry)

		// FORMAT ZLEX: Ghép SID (20 số zero-padding) vào trước data
		// Ví dụ: "00000123456789012345:{"id":"..."}"
		member := fmt.Sprintf("%020d:%s", sid, eventJSON)

		rdb.ZAdd(ctx, ActiveEvents, redis.Z{
			Score:  0, // Tất cả Score = 0 để ép Redis sort theo Member (ZLEX)
			Member: member,
		})
	}

	select {
	case packSignal <- struct{}{}:
	default:
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Đã tạo %d bản ghi", count)
}

// --- LUỒNG WORKER ĐÓNG GÓI ---
func chunkPackerWorker() {
	for range packSignal {
		packChunkIfNeeded()
	}
}

func packChunkIfNeeded() {
	locked, err := rdb.SetNX(ctx, PackLockKey, "1", 30*time.Second).Result()
	if err != nil || !locked {
		return // Không lấy được lock, bỏ qua
	}
	defer rdb.Del(ctx, PackLockKey)

	for {
		count := rdb.ZCard(ctx, ActiveEvents).Val()
		if count < ChunkSize {
			break
		}

		// ZRANGE mặc định vẫn đúng vì Score=0 nó tự lấy theo Lexicographical từ nhỏ đến lớn
		eventsStr, _ := rdb.ZRange(ctx, ActiveEvents, 0, ChunkSize-1).Result()

		var events []model.EventLog
		var maxSIDStr string
		var maxSID uint64

		for _, evStr := range eventsStr {
			// Tách "0000...sid" và chuỗi "JSON"
			parts := strings.SplitN(evStr, ":", 2)
			if len(parts) != 2 {
				continue
			}

			maxSIDStr = parts[0] // Phần tử cuối sẽ có SID lớn nhất

			var ev model.EventLog
			json.Unmarshal([]byte(parts[1]), &ev)
			events = append(events, ev)
			maxSID = ev.SID
		}

		// Tạo tên chunk và lưu JSON
		chunkKey := fmt.Sprintf("%s:chunk:%d", AppPrefix, maxSID)
		chunkJSON, _ := json.Marshal(events)
		rdb.Set(ctx, chunkKey, chunkJSON, 7*24*time.Hour) // Sống 7 ngày trên RAM

		// Lưu Registry: Đưa SID lên đầu để dùng ZLEX
		registryMember := fmt.Sprintf("%020d:%s", maxSID, chunkKey)
		rdb.ZAdd(ctx, ChunkRegistry, redis.Z{
			Score:  0,
			Member: registryMember,
		})

		// XOÁ THEO ZLEX (Cực kỳ an toàn):
		// Xoá từ giá trị nhỏ nhất ("-") đến SID lớn nhất (bao gồm cả nó và các ký tự đính kèm bằng cách cộng "\xff")
		rdb.ZRemRangeByLex(ctx, ActiveEvents, "-", "["+maxSIDStr+":\xff")

		log.Printf("Đã đóng gói thành công: %s", chunkKey)
	}
}

// --- API 2: Lấy dữ liệu (Đồng bộ) ---
func syncHandler(w http.ResponseWriter, r *http.Request) {
	lastIDStrQuery := r.URL.Query().Get("last_id")
	lastID, err := strconv.ParseUint(lastIDStrQuery, 10, 64)
	if err != nil {
		lastID = 0
	}

	w.Header().Set("Content-Type", "application/json")

	// FORMAT QUERY THEO ZLEX: Tạo chuỗi min cực chuẩn
	// lastIDStr = "00000000000123456789"
	lastIDStr := fmt.Sprintf("%020d", lastID)
	// "(" nghĩa là lớn hơn hẳn (không lấy dấu bằng)
	minLex := "(" + lastIDStr + ":"

	// 1. TÌM TRONG CHUNK REGISTRY BẰNG ZRANGEBYLEX
	chunks, _ := rdb.ZRangeByLex(ctx, ChunkRegistry, &redis.ZRangeBy{
		Min: minLex, Max: "+", Offset: 0, Count: 1, // "+" là giá trị lớn nhất vô cực
	}).Result()

	if len(chunks) > 0 {
		// chunks[0] có dạng "00000123456789012345:scam_alert:chunk:123456..."
		parts := strings.SplitN(chunks[0], ":", 2)
		chunkKey := parts[1]

		chunkData, _ := rdb.Get(ctx, chunkKey).Result()
		var allEvents []model.EventLog
		json.Unmarshal([]byte(chunkData), &allEvents)

		var filtered []model.EventLog
		for _, ev := range allEvents {
			if ev.SID > lastID { // Vẫn dùng uint64 để filter trên Memory Go rất nhanh
				filtered = append(filtered, ev)
			}
		}
		json.NewEncoder(w).Encode(filtered)
		return
	}

	// 2. NẾU KHÔNG CÓ TRONG REGISTRY -> TÌM TRONG ACTIVE EVENTS
	activeEventsStr, _ := rdb.ZRangeByLex(ctx, ActiveEvents, &redis.ZRangeBy{
		Min: minLex, Max: "+", Offset: 0, Count: ChunkSize,
	}).Result()

	var active []model.EventLog
	for _, evStr := range activeEventsStr {
		parts := strings.SplitN(evStr, ":", 2)
		if len(parts) == 2 {
			var ev model.EventLog
			json.Unmarshal([]byte(parts[1]), &ev)
			active = append(active, ev)
		}
	}

	json.NewEncoder(w).Encode(active)
}
