package util

import (
	"fmt"
	"math/rand"
	"redis-stream-demo/src/model"
	"time"

	"github.com/sony/sonyflake"
)

// Hàm random string: Tạo chuỗi random cho trường "id" (giống format ES/Firestore) ---
func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func GenerateData(sf *sonyflake.Sonyflake, count int) []model.EventLog {
	carriers := []string{"Viettel", "Mobifone", "Vinaphone", "Việt Nam"}
	categories := []string{"scam", "spam", "suspectSpam", "suspectScam"}
	actions := []string{"update", "add"}

	logsEntry := []model.EventLog{}

	for i := 1; i <= count; i++ {
		id, _ := sf.NextID()

		// Tạo Mock Object khớp với JSON mẫu
		logEntry := model.EventLog{
			ID: RandomString(20), // randomString(20), // VD: 6QawtpwB-xMT8VeR6JaW
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

		logsEntry = append(logsEntry, logEntry)
	}
	return logsEntry
}
