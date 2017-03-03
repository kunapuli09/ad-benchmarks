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

}

//sample redis test
func TestRedis(t *testing.T) {
	t.Skip()
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0, // use default DB
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	// Output: PONG <nil>
	err1 := client.Set("key", 2, 0).Err()
	if err1 != nil {
		t.Fail()
	}

	val, err2 := client.Get("key").Result()
	if err2 != nil {
		t.Fail()
	}
	fmt.Println("key", val)

	val2, err3 := client.Get("key2").Result()
	if err3 == redis.Nil {
		fmt.Println("key2 does not exists")
	} else if err3 != nil {
		t.Fail()
	} else {
		fmt.Println("key2", val2)
	}
	// Output: key value
	// key2 does not exists
}
