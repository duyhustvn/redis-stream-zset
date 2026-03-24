package combine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"redis-stream-demo/src/config"
	"redis-stream-demo/src/middleware"
	"redis-stream-demo/src/model"
	redisclient "redis-stream-demo/src/pkg/redis"
	"redis-stream-demo/src/pkg/util"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/sony/sonyflake"
)

// Cấu hình Redis & Constants

const (
	ChunkSize = 15000
)

var AppPrefix string
var ActiveEvents string
var ChunkRegistry string
var PackLockKey string

var (
	rdb                  *redis.Client
	sf                   *sonyflake.Sonyflake
	ctx                  = context.Background()
	packSignal           = make(chan struct{}, 1) // Channel kích hoạt worker
	carriers             = []string{"Viettel", "Mobifone", "Vinaphone", "Vietnamobile"}
	categories           = []string{"SCAM", "SPAM", "TELEMARKETING", "DEBT_COLLECTION"}
	actions              = []string{"ADD", "UPDATE", "DELETE"}
	syncLatencyBuckets   = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30}
	chunkSyncAPIDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "redis_trunk",
		Subsystem: "chunk_sync",
		Name:      "api_duration_seconds",
		Help:      "Total API duration for /api/sync in chunk mode.",
		Buckets:   syncLatencyBuckets,
	}, []string{"source", "status"})
	chunkSyncRedisDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "redis_trunk",
		Subsystem: "chunk_sync",
		Name:      "redis_duration_seconds",
		Help:      "Accumulated Redis time spent by /api/sync in chunk mode.",
		Buckets:   syncLatencyBuckets,
	}, []string{"source", "status"})
	chunkSyncRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_trunk",
		Subsystem: "chunk_sync",
		Name:      "requests_total",
		Help:      "Total number of /api/sync requests in chunk mode.",
	}, []string{"source", "status"})
	chunkSyncRecordsReturned = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "redis_trunk",
		Subsystem: "chunk_sync",
		Name:      "records_returned",
		Help:      "Number of records returned per /api/sync request in chunk mode.",
		Buckets:   []float64{0, 1, 10, 100, 500, 1000, 5000, 10000, 15000},
	}, []string{"source"})
)

func init() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Cannot load env: %+v", err)
	}

	if cfg.RedisConfig.EnableGzip {
		AppPrefix = "event_gzip"
		fmt.Println("Enable Gzip")
	} else {
		AppPrefix = "event"
		fmt.Println("NOT Enable Gzip yet")
	}

	ActiveEvents = AppPrefix + ":active_events"
	ChunkRegistry = AppPrefix + ":chunk_registry"
	PackLockKey = AppPrefix + "lock:pack_chunk"

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

type handler struct {
	config *config.Config
}

func NewHandler(cfg *config.Config) *handler {
	return &handler{
		config: cfg,
	}
}

func (inst *handler) Routes() {
	go chunkPackerWorker(*inst.config)

	http.HandleFunc("/api/generate", inst.generateHandler)
	http.HandleFunc("/api/sync", middleware.GzipMiddleware(inst.syncHandler))
	http.HandleFunc("/api/stats", inst.handleStats)
	http.HandleFunc("/api/test/setup", inst.handleTestSetup)
	http.Handle("/metrics", promhttp.Handler())

	fmt.Println("Server đang chạy tại http://:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func setSyncTimingHeaders(w http.ResponseWriter, source string, redisQueryTime, apiDuration time.Duration) {
	redisMs := float64(redisQueryTime) / float64(time.Millisecond)
	apiMs := float64(apiDuration) / float64(time.Millisecond)

	w.Header().Set("X-Redis-Time-Ms", fmt.Sprintf("%.3f", redisMs))
	w.Header().Set("X-Api-Time-Ms", fmt.Sprintf("%.3f", apiMs))
	w.Header().Set("X-Sync-Source", source)
	w.Header().Set("Server-Timing", fmt.Sprintf("redis;dur=%.3f, app;dur=%.3f", redisMs, apiMs))
}

func observeSyncMetrics(source, status string, redisQueryTime, apiDuration time.Duration, recordsReturned int) {
	chunkSyncAPIDuration.WithLabelValues(source, status).Observe(apiDuration.Seconds())
	chunkSyncRedisDuration.WithLabelValues(source, status).Observe(redisQueryTime.Seconds())
	chunkSyncRequestsTotal.WithLabelValues(source, status).Inc()
	chunkSyncRecordsReturned.WithLabelValues(source).Observe(float64(recordsReturned))
}

// --- API 1: Generate Data ---
func (inst *handler) generateHandler(w http.ResponseWriter, r *http.Request) {
	countStr := r.URL.Query().Get("count")
	count, err := strconv.Atoi(countStr)
	if err != nil || count <= 0 {
		http.Error(w, "Tham số count không hợp lệ", http.StatusBadRequest)
		return
	}

	logsEntry := util.GenerateData(sf, count)
	for _, logEntry := range logsEntry {
		eventJSON, _ := json.Marshal(logEntry)

		// FORMAT ZLEX: Ghép SID (20 số zero-padding) vào trước data
		// Ví dụ: "00000123456789012345:{"id":"..."}"
		member := fmt.Sprintf("%020d:%s", logEntry.SID, eventJSON)

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
func chunkPackerWorker(cfg config.Config) {
	for range packSignal {
		packChunkIfNeeded(cfg)
	}
}

func packChunkIfNeeded(cfg config.Config) {
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

		if cfg.RedisConfig.EnableGzip {
			// Gzip data before save into redis
			var b bytes.Buffer
			gz := gzip.NewWriter(&b)
			gz.Write(chunkJSON)
			gz.Close() // flush data

			rdb.Set(ctx, chunkKey, b.Bytes(), 7*24*time.Hour)
		} else {
			rdb.Set(ctx, chunkKey, chunkJSON, 7*24*time.Hour)
		}

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

type SyncResponse struct {
	Count int              `json:"count"`
	Data  []model.EventLog `json:"data"`
}

// --- API 2: Lấy dữ liệu (Đồng bộ) ---
func (inst *handler) syncHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Đánh dấu thời điểm bắt đầu API
	apiStartTime := time.Now()
	source := "registry"
	status := "ok"
	recordsReturned := 0
	var redisQueryTime time.Duration // Biến lưu tổng thời gian gọi Redis
	defer func() {
		apiDuration := time.Since(apiStartTime)
		observeSyncMetrics(source, status, redisQueryTime, apiDuration, recordsReturned)
	}()

	lastIDStrQuery := r.URL.Query().Get("last_id")
	lastID, err := strconv.ParseUint(lastIDStrQuery, 10, 64)
	if err != nil {
		lastID = 0
	}

	w.Header().Set("Content-Type", "application/json")

	lastIDStr := fmt.Sprintf("%020d", lastID)
	minLex := "(" + lastIDStr + ":"

	var resultEvents []model.EventLog

	// 2. Đo thời gian query Registry
	redisStartTime := time.Now()
	chunks, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:    ChunkRegistry,
		Start:  minLex,
		Stop:   "+",
		ByLex:  true,
		Offset: 0,
		Count:  1,
	}).Result()
	redisQueryTime += time.Since(redisStartTime)
	if err != nil {
		status = "error"
		log.Printf("Query Registry Redis failed: %+v", err)
		setSyncTimingHeaders(w, source, redisQueryTime, time.Since(apiStartTime))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(chunks) > 0 {
		source = "chunk"
		parts := strings.SplitN(chunks[0], ":", 2)
		chunkKey := parts[1]

		var allEvents []model.EventLog
		// Đo thêm thời gian GET dữ liệu chunk từ Redis
		getChunkStartTime := time.Now()
		if inst.config.RedisConfig.EnableGzip {
			chunkDataCompressed, err := rdb.Get(ctx, chunkKey).Bytes()
			redisQueryTime += time.Since(getChunkStartTime)
			if err != nil {
				status = "error"
				log.Printf("Query Chunk Redis failed: %+v", err)
				setSyncTimingHeaders(w, source, redisQueryTime, time.Since(apiStartTime))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			gr, err := gzip.NewReader(bytes.NewReader(chunkDataCompressed))
			if err != nil {
				status = "error"
				log.Printf("Create gzip reader for chunk failed: %+v", err)
				setSyncTimingHeaders(w, source, redisQueryTime, time.Since(apiStartTime))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			uncompressedJSON, err := io.ReadAll(gr)
			gr.Close()
			if err != nil {
				status = "error"
				log.Printf("Read gzip chunk failed: %+v", err)
				setSyncTimingHeaders(w, source, redisQueryTime, time.Since(apiStartTime))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if err := json.Unmarshal(uncompressedJSON, &allEvents); err != nil {
				status = "error"
				log.Printf("Unmarshal gzip chunk failed: %+v", err)
				setSyncTimingHeaders(w, source, redisQueryTime, time.Since(apiStartTime))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		} else {
			chunkData, err := rdb.Get(ctx, chunkKey).Result()
			redisQueryTime += time.Since(getChunkStartTime)
			if err != nil {
				status = "error"
				log.Printf("Query Chunk Redis failed: %+v", err)
				setSyncTimingHeaders(w, source, redisQueryTime, time.Since(apiStartTime))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := json.Unmarshal([]byte(chunkData), &allEvents); err != nil {
				status = "error"
				log.Printf("Unmarshal chunk failed: %+v", err)
				setSyncTimingHeaders(w, source, redisQueryTime, time.Since(apiStartTime))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		for _, ev := range allEvents {
			if ev.SID > lastID {
				resultEvents = append(resultEvents, ev)
			}
		}
	} else {
		source = "active"
		// NẾU KHÔNG CÓ TRONG REGISTRY -> TÌM TRONG ACTIVE EVENTS
		activeStartTime := time.Now()
		activeEventsStr, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:    ActiveEvents,
			Start:  minLex,
			Stop:   "+",
			ByLex:  true,
			Offset: 0,
			Count:  int64(ChunkSize), // go-redis yêu cầu int64 cho Count
		}).Result()
		redisQueryTime += time.Since(activeStartTime)
		if err != nil {
			status = "error"
			log.Printf("Query Acive Events Redis failed: %+v", err)
			setSyncTimingHeaders(w, source, redisQueryTime, time.Since(apiStartTime))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Khai báo kiểu RawMessage để Go bỏ qua bước Unmarshal/Marshal tốn kém
		var active []json.RawMessage
		for _, evStr := range activeEventsStr {
			parts := strings.SplitN(evStr, ":", 2)
			if len(parts) == 2 {
				// var ev model.EventLog
				// json.Unmarshal([]byte(parts[1]), &ev)
				// resultEvents = append(resultEvents, ev)

				// Member: 00116650985607593988:{"id":"PjZcrDKFhpUBNcmUXnXU","phoneNumber":{"value":"+84942810879","carrier":"Mobifone","category":"TELEMARKETING","risk_level":100,"meta":{"subCategory":"spam"},"user_risk_score":12},"type":"UPDATE","createdTime":1773596605,"sid":116650985607593988}
				// parts[1] chính là chuỗi '{"id":"...","phoneNumber":{...},...}'
				// Ép kiểu nó sang RawMessage và nhét thẳng vào mảng kết quả
				active = append(active, json.RawMessage(parts[1]))
			}
		}

		// Tạo Response động vì kiểu dữ liệu data giờ là []json.RawMessage
		response := map[string]interface{}{
			"count": len(active),
			"data":  active,
		}

		recordsReturned = len(active)
		apiDuration := time.Since(apiStartTime)
		setSyncTimingHeaders(w, source, redisQueryTime, apiDuration)
		json.NewEncoder(w).Encode(response)

		log.Printf("[SYNC API - ACTIVE] last_id: %d | Records: %d | Redis Time: %v | Total API Time: %v",
			lastID, len(active), redisQueryTime, apiDuration)
		return
	}

	// Tránh trả về null nếu không có dữ liệu, khởi tạo mảng rỗng
	if resultEvents == nil {
		resultEvents = []model.EventLog{}
	}

	// 3. Format Response mới thêm thuộc tính Count
	response := SyncResponse{
		Count: len(resultEvents),
		Data:  resultEvents,
	}

	recordsReturned = response.Count
	apiDuration := time.Since(apiStartTime)
	setSyncTimingHeaders(w, source, redisQueryTime, apiDuration)
	json.NewEncoder(w).Encode(response)

	// 4. Tính toán tổng thời gian API và Log ra console
	log.Printf("[SYNC API] last_id: %d | Records: %d | Redis Time: %v | Total API Time: %v",
		lastID, response.Count, redisQueryTime, apiDuration)
}

// --- API 3: Cấp phát ID hợp lệ cho k6 Load Test ---
// Method: GET /api/test/setup
func (inst *handler) handleTestSetup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Lấy 500 bản ghi cũ nhất (nằm ở đáy ZSET) của ActiveEvents
	// Lệnh ZRange mặc định lấy theo index (từ 0 đến 500), kết hợp với Score=0 nó sẽ tự lấy theo ZLEX cực chuẩn
	members, err := rdb.ZRange(ctx, ActiveEvents, 0, 500).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// DÙNG MẢNG STRING ĐỂ TRÁNH LỖI 53-BIT TRÊN K6 (JS ENGINE)
	validIDs := []string{"0"} // Luôn khởi tạo với "0" cho kịch bản New User
	// validIDs := []string{} // Luôn khởi tạo với "0" cho kịch bản New User

	for _, member := range members {
		parts := strings.SplitN(member, ":", 2)
		if len(parts) > 0 {
			// Ép qua uint64 rồi format lại string để loại bỏ các số "0" dư thừa ở đầu (zero-padding)
			sfid, _ := strconv.ParseUint(parts[0], 10, 64)
			validIDs = append(validIDs, strconv.FormatUint(sfid, 10))
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"sids": validIDs,
	})
}

// --- API 4: Thống kê toàn diện hệ thống (Records & RAM) ---
// Method: GET /api/stats
func (inst *handler) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Helper function để format Byte sang Megabyte cho dễ nhìn
	toMB := func(bytes int64) string {
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
	}

	// 1. Quét nhánh Active Events
	activeRecords, _ := rdb.ZCard(ctx, ActiveEvents).Result()
	activeMem, _ := rdb.MemoryUsage(ctx, ActiveEvents).Result()

	// 2. Quét nhánh Chunk Registry (Sổ đăng ký)
	registryChunks, _ := rdb.ZCard(ctx, ChunkRegistry).Result()
	registryMem, _ := rdb.MemoryUsage(ctx, ChunkRegistry).Result()

	// 3. Quét chi tiết dung lượng của từng Chunk Data
	var chunksMem int64
	members, _ := rdb.ZRange(ctx, ChunkRegistry, 0, -1).Result()

	for _, member := range members {
		parts := strings.SplitN(member, ":", 2)
		if len(parts) == 2 {
			chunkKey := parts[1]
			// Đo dung lượng của từng Key chứa JSON String
			mem, err := rdb.MemoryUsage(ctx, chunkKey).Result()
			// Bỏ qua lỗi nếu Key đã bị Redis xoá do hết hạn (TTL)
			if err == nil {
				chunksMem += mem
			}
		}
	}

	// Số bản ghi trong Chunk = Số lượng chunk * 15.000
	chunksRecords := registryChunks * int64(ChunkSize)

	// 4. Tổng hợp toàn hệ thống
	totalRecords := activeRecords + chunksRecords
	totalMem := activeMem + registryMem + chunksMem

	// 5. Trả về JSON Report
	response := map[string]interface{}{
		"active_events": map[string]interface{}{
			"records":      activeRecords,
			"memory_bytes": activeMem,
			"memory_mb":    toMB(activeMem),
		},
		"chunk_registry": map[string]interface{}{
			"chunks":       registryChunks,
			"memory_bytes": registryMem,
			"memory_mb":    toMB(registryMem),
		},
		"chunks_data": map[string]interface{}{
			"records":      chunksRecords,
			"memory_bytes": chunksMem,
			"memory_mb":    toMB(chunksMem),
		},
		"system_total": map[string]interface{}{
			"total_records":      totalRecords,
			"total_memory_bytes": totalMem,
			"total_memory_mb":    toMB(totalMem),
		},
	}

	json.NewEncoder(w).Encode(response)
}
