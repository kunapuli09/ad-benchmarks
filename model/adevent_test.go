package model

import "testing"
import (
	"fmt"
	"gopkg.in/redis.v5"
)

func TestParseJson(t *testing.T) {
	event := &AdEvent{}
	jsonStr := `{
	"event_time":"123456",
	"ad_type": "web",
	"event_type" : "test",
	"ip_address" : "1.2.3.4",
	"user_id" : "1234",
	"page_id" : "800",
	"ad_id" : "123"
	}`

	err := event.ParseJson([]byte(jsonStr))
	if err != nil {
		t.Fail()
	}
	fmt.Println("event_time=", event.EventTime)
	fmt.Println("event_type=", event.EventType)

}

//sample redis test
func TestRedis(t *testing.T) {
	t.Skip()
	client := redis.NewClient(&redis.Options{
		Addr:     "10.0.0.131:6379",
		Password: "", // no password set
		DB:       0, // use default DB
	})
	
	val, err := client.Get("428b3221-11d6-4c32-99dd-17ebeecae67f").Result()
	
	if err == redis.Nil {
		fmt.Println("campaigns does not exists", err)
	} else if err != nil {
		t.Errorf("error", err)
		t.Fail()
	} else {
		fmt.Println("campaigns", val)
	}
	// Output: key value
	// key2 does not exists
}
