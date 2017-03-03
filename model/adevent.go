package model

import (
	"fmt"

	"encoding/json"
	"github.com/kunapuli09/ad-benchmarks/common"
)

//input event
type AdEvent struct {
	AdType    string `json:"ad_type"`
	EventType string `json:"event_type"`
	IPAddress string `json:"ip_address"`
	EventTime string `json:event_time`
	UserId    string    `json:"user_id"`
	PageId    string    `json:"page_id"`
	AdId      string    `json:"ad_id"`
}

//filtered event
type ProjectedEvent struct {
	EventTime string
	AdId      string
}

//redis join event
type EnrichedEvent struct {
	CampaignId string
	AdId       string
	EventTime  string
}

//metric information that's stored in redis
type MetricEvent struct {
	CampaignId string
	Metric     *common.Window
}

func (event *AdEvent) ParseJson(msg []byte) error {
	err := json.Unmarshal(msg, event)
	if err != nil {
		fmt.Println("error:", err)
		return err
	}
	return nil
}