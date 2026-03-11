package model

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
