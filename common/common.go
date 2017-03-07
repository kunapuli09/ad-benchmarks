package common

import (
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"gopkg.in/redis.v5"
	"log"
	"os"
	"strconv"
	"time"
	"sync"
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
	CampaignWindows  map[int64]map[string]*Window
	NeedFlush        []*CampaignWindowPair
}

func NewRedisDB() *RedisDB {
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
		QueryClient:      db,
		FlushClient:      db,
		LastWindowMillis: time.Now().UnixNano() / int64(time.Millisecond),
		ProcessedCount:   int64(0),
		CampaignWindows:  make(map[int64]map[string]*Window, 10),
		NeedFlush:        make([]*CampaignWindowPair, 10000),
	}
}

func (processor *CampaignProcessor) Prepare() {
	//TODO move this to an Docker ENV variable
	flushTicker := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-flushTicker.C:
			//flush the cache
			processor.flushWindows()
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (processor *CampaignProcessor) Execute(campaignId string, eventTime string) {
	event_time, err := strconv.ParseInt(eventTime, 10, 64)
	if nil != err {
		timeBucket := event_time / time_divisor
		window := processor.getWindow(timeBucket, campaignId)
		window.SeenCount++
		newPair := NewCampaignWindowPair(campaignId, window)
		processor.NeedFlush = append(processor.NeedFlush, newPair)
		processor.ProcessedCount++
	}

}

func (processor *CampaignProcessor) writeWindow(campaign string, win *Window) error {
	var windowUUID string
	var windowListUUID string
	val, err := processor.FlushClient.conn.HMGet(campaign, win.Timestamp).Result()
	if err == redis.Nil {
		return err
	} else if err != nil {
		return err
	}
	if nil == val {
		return errors.New("windowsUUID is nil")
	} else {
		//log.Printf("Cache hit -- windowsUUID=%v", val[0])
		if nil != val[0]{
			val0, ok := val[0].(string)
			if ok{
				windowUUID = val0
			}else{
				windowUUID = ""
			}
		}
		
	}
	if windowUUID == "" {
		windowUUID = uuid.NewV4().String()
		processor.FlushClient.conn.HSet(campaign, win.Timestamp, windowUUID)
		val1, err := processor.FlushClient.conn.HMGet(campaign, "windows").Result()
		if err == redis.Nil {
			return err
		} else if err != nil {
			return err
		}
		if nil == val1 {
			return errors.New("windowListUUID is nil")
		} else {
			if nil != val[0]{
				val10, ok := val1[0].(string)
				if ok{
					windowUUID = val10
				}else{
					windowUUID = ""
				}
			}
			
		}
		if windowListUUID == "" {
			windowListUUID = uuid.NewV4().String()
			processor.FlushClient.conn.HSet(campaign, "windows", windowListUUID)
		}
		processor.FlushClient.conn.LPush(windowListUUID, win.Timestamp)
	}
	processor.FlushClient.conn.HIncrBy(windowUUID, "seen_count", win.SeenCount)
	win.SeenCount = 0
	processor.FlushClient.conn.HSet(windowUUID, "time_updated", time.Now().UnixNano()/int64(time.Millisecond))
	processor.FlushClient.conn.LPush("time_updated", time.Now().UnixNano()/int64(time.Millisecond))
	return nil
}

func (processor *CampaignProcessor) flushWindows() {
	for _, pair := range processor.NeedFlush {
		if pair != nil {
			processor.writeWindow(pair.Campaign, pair.CampaignWindow)
		}
	}
}

func (processor *CampaignProcessor) redisGetWindow(timeBucket int64, timeDivisor int64) *Window {
	return NewWindow(fmt.Sprintf("%d", timeBucket* time_divisor), 0)
}

func (processor *CampaignProcessor) getWindow(timeBucket int64, campaignId string) *Window {
	bucketMap := processor.CampaignWindows[timeBucket]
	if bucketMap == nil {
		//redis server lookup
		redisWindow := processor.redisGetWindow(timeBucket, time_divisor)
		if redisWindow != nil {
			bucketMap = make(map[string]*Window)
			processor.CampaignWindows[timeBucket] = bucketMap
			bucketMap[campaignId] = redisWindow
			return redisWindow
		}
		bucketMap = make(map[string]*Window)
		processor.CampaignWindows[timeBucket] = bucketMap
	}
	// Bucket exists. Check the window.
	window := bucketMap[campaignId]
	if window == nil {
		// Try to pull from redis into cache.
		redisWindow := processor.redisGetWindow(timeBucket, time_divisor)
		if redisWindow != nil {
			bucketMap[campaignId] = redisWindow
			return redisWindow
		}
		// Otherwise, if nothing in redis:
		window = NewWindow(fmt.Sprintf("%d", timeBucket* time_divisor), 0)
		bucketMap[campaignId] = redisWindow

	}
	return window

}

type RedisAdCampaignCache struct {
	QueryClient  *RedisDB
	AdToCampaign map[string]string
	AdLock *sync.RWMutex
}

func NewRedisAdCampaignCache(db *RedisDB) *RedisAdCampaignCache {
	return &RedisAdCampaignCache{
		QueryClient:  db,
		AdToCampaign: make(map[string]string),
		AdLock: &sync.RWMutex{},
	}
}

//Map access fails with concurrent go routines or multiple processes
//add mutexe locks
func (cache *RedisAdCampaignCache) Execute(adId string) string {
	var localCampaignId string
	var exists bool
	cache.AdLock.RLock()
	//access local map
	if localCampaignId, exists = cache.AdToCampaign[adId]; !exists{
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
		}else{
			localCampaignId = campaignId
		}
		cache.AdToCampaign[adId] = campaignId
		cache.AdLock.Unlock()
		
	}else{
		cache.AdLock.RLock()
	}
	return localCampaignId
}
