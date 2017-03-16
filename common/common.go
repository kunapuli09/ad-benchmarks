package common

import (
	"fmt"
	"github.com/satori/go.uuid"
	"gopkg.in/redis.v5"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

/**
time series logic to bucket and store events to redis
influxdb would be great to address this problem
*/
const (
	time_divisor int64 = 10000
)

type RedisDB struct {
	conn *redis.Client
}

type Window struct {
	Timestamp string
	SeenCount int64
}

type CampaignWindowPair struct {
	Campaign       string
	CampaignWindow *Window
}

type CampaignProcessor struct {
	QueryClient      *RedisDB
	FlushClient      *RedisDB
	LastWindowMillis int64
	ProcessedCount   int64
	// TimeBucket -> (CampaignId -> Window)
	CampaignWindows    map[int64]map[string]*Window
	NeedFlush          chan *CampaignWindowPair
	CampaignWindowLock *sync.RWMutex
}

func NewRedisDB() *RedisDB {
	os.Setenv("redis_url", "10.0.0.131:6379")
	os.Setenv("redis_database", "ads")
	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("redis_url"),
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return &RedisDB{client}
}

func NewCampaignWindowPair(campaign string, window *Window) *CampaignWindowPair {
	return &CampaignWindowPair{campaign, window}
}

func NewWindow(timestamp string, seenCount int64) *Window {
	return &Window{timestamp, seenCount}
}

func NewCampaignProcessor(db *RedisDB) *CampaignProcessor {
	return &CampaignProcessor{
		QueryClient:        db,
		FlushClient:        db,
		LastWindowMillis:   time.Now().UnixNano() / int64(time.Millisecond),
		ProcessedCount:     int64(0),
		CampaignWindows:    make(map[int64]map[string]*Window, 10),
		NeedFlush:          make(chan *CampaignWindowPair, 10000),
		CampaignWindowLock: &sync.RWMutex{},
	}
}

func (processor *CampaignProcessor) Prepare() {
	pairs := []*CampaignWindowPair{}
	//TODO move this to an Docker ENV variable
	ticker := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case pair, ok := <- processor.NeedFlush:
			if ok {
				pairs = append(pairs, pair)
			} else {
				log.Printf("Inbound pair channel has closed, terminating processor")
				return
			}
		case <-ticker.C:
			processor.flushWindows(pairs)
			pairs = []*CampaignWindowPair{}
		}
	}
}

func (processor *CampaignProcessor) flushWindows(pairs []*CampaignWindowPair) {
	for _, pair := range pairs {
		if pair != nil {
			processor.writeWindow(pair.Campaign, pair.CampaignWindow)
		}
	}
}

func (processor *CampaignProcessor) Execute(campaignId string, eventTime string) {
	event_time, err := strconv.ParseInt(eventTime, 10, 64)
	if nil != err {
		timeBucket := event_time / time_divisor
		window := processor.getWindow(timeBucket, campaignId)
		if window != nil{
			window.SeenCount++
			newPair := NewCampaignWindowPair(campaignId, window)
			processor.NeedFlush <- newPair
			processor.ProcessedCount++
		}
		
	}

}


func (processor *CampaignProcessor) writeWindow(campaign string, win *Window) error {
	processor.CampaignWindowLock.RLock()
	val, err := processor.FlushClient.conn.HMGet(campaign, win.Timestamp).Result()
	windowUUID := processor.getFirstValue(val, err)
	if windowUUID == "" {
		processor.CampaignWindowLock.RUnlock()
		windowUUID = uuid.NewV4().String()
		processor.CampaignWindowLock.RLock()
		processor.FlushClient.conn.HSet(campaign, win.Timestamp, windowUUID)
		val1, err := processor.FlushClient.conn.HMGet(campaign, "windows").Result()
		windowListUUID := processor.getFirstValue(val1, err)
		if windowListUUID == "" {
			windowListUUID = uuid.NewV4().String()
			processor.FlushClient.conn.HSet(campaign, "windows", windowListUUID)
		}
		processor.FlushClient.conn.LPush(windowListUUID, win.Timestamp)
		processor.CampaignWindowLock.RUnlock()
	} else {
		processor.CampaignWindowLock.RUnlock()
	}
	processor.CampaignWindowLock.RLock()
	processor.FlushClient.conn.HIncrBy(windowUUID, "seen_count", win.SeenCount)
	win.SeenCount = 0
	processor.FlushClient.conn.HSet(windowUUID, "time_updated", time.Now().UnixNano()/int64(time.Millisecond))
	processor.FlushClient.conn.LPush("time_updated", time.Now().UnixNano()/int64(time.Millisecond))
	processor.CampaignWindowLock.RUnlock()
	return nil
}

func (processor *CampaignProcessor) redisGetWindow(timeBucket int64, timeDivisor int64) *Window {
	return NewWindow(fmt.Sprintf("%d", timeBucket*time_divisor), 0)
}

func (processor *CampaignProcessor) getWindow(timeBucket int64, campaignId string) *Window {
	var window *Window
	var present bool
	bucketMap := processor.getBucketMapForTime(timeBucket)
	// Bucket exists. Check the window.
	processor.CampaignWindowLock.RLock()
	if window, present = bucketMap[campaignId]; !present {
		processor.CampaignWindowLock.RUnlock()
		// Try to pull from redis into cache.
		processor.CampaignWindowLock.Lock()
		redisWindow := processor.redisGetWindow(timeBucket, time_divisor)
		bucketMap[campaignId] = redisWindow
		processor.CampaignWindowLock.Unlock()
	} else {
		processor.CampaignWindowLock.RUnlock()
	}
	return window
}


func (processor *CampaignProcessor) getBucketMapForTime(timeBucket int64) map[string]*Window {
	var bucketMap map[string]*Window
	var present bool
	processor.CampaignWindowLock.RLock()
	if bucketMap, present = processor.CampaignWindows[timeBucket]; !present {
		processor.CampaignWindowLock.RUnlock()
		processor.CampaignWindowLock.Lock()
		bucketMap = make(map[string]*Window)
		processor.CampaignWindows[timeBucket] = bucketMap
		processor.CampaignWindowLock.Unlock()
	} else {
		processor.CampaignWindowLock.RUnlock()
	}
	return bucketMap
}

//type check this
func (processor *CampaignProcessor) getFirstValue(val []interface{}, err error) string {
	if err != redis.Nil && err != nil && val != nil {
		if nil != val[0] {
			val0, ok := val[0].(string)
			if ok {
				return val0
			} else {
				return ""
			}
		}
	}
	return ""
}


type RedisAdCampaignCache struct {
	QueryClient  *RedisDB
	AdToCampaign map[string]string
	AdLock       *sync.RWMutex
}

func NewRedisAdCampaignCache(db *RedisDB) *RedisAdCampaignCache {
	return &RedisAdCampaignCache{
		QueryClient:  db,
		AdToCampaign: make(map[string]string),
		AdLock:       &sync.RWMutex{},
	}
}

//Map access fails with concurrent go routines or multiple processes
//add mutex locks
func (cache *RedisAdCampaignCache) Execute(adId string) string {
	var localCampaignId string
	var exists bool
	cache.AdLock.RLock()
	//access local map
	if localCampaignId, exists = cache.AdToCampaign[adId]; !exists {
		cache.AdLock.RUnlock()
		cache.AdLock.Lock()
		//hit redis cache
		campaignId, err := cache.QueryClient.conn.Get(adId).Result()
		if err == redis.Nil {
			//log.Println("Redis query for CampaignId is Nil")
			localCampaignId = ""
		} else if err != nil {
			log.Printf("Redis query for CampaignId is %v", err)
			localCampaignId = ""
		} else {
			localCampaignId = campaignId
		}
		cache.AdToCampaign[adId] = campaignId
		cache.AdLock.Unlock()

	} else {
		cache.AdLock.RLock()
	}
	return localCampaignId
}
