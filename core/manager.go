package core

import (
	"fmt"
	"log"
	"time"

	"os"
	"strconv"
	
	"github.com/kunapuli09/ad-benchmarks/common"
	"github.com/kunapuli09/ad-benchmarks/model"
	"github.com/bsm/sarama-cluster"
	
	"github.com/Shopify/sarama"
)

//Workflow
//Filter the records if event type is "view"
//project the event, basically filter the fileds. ad_id and event_time fields
//query Redis TopLevel using projected event
// create new local map ad_to_campaign with(string,string) key:ad_id, value: campaign_id
// check to see if ad_id is null in the local map -- it's a miss
// check to see if ad_id exists in the redis cache
// if exists, add campaign_id to local map
// return an object with campaing_id, ad_id and event_time
//build a map with key: (campaign_id, window_time) value : ad_id
//window_time is event_time/10000
//count total events by key:(campaign_id, window_time) -- use map-reduce
//write the counts to redis cache

type Manager struct {
	consumer        *cluster.Consumer
	processor       *common.CampaignProcessor
	eventsTopic     string
	workers         int
	restartInterval time.Duration
	logInterval     time.Duration
	redis           *common.RedisDB
	//channel for persisted events with SeenCount metric
	metrics chan *model.MetricEvent
	//input channel for kafka events
	input chan *model.AdEvent
	//channel to filter and project
	filter chan *model.AdEvent
	//channel for data lookup& join with redis data
	enrich chan *model.ProjectedEvent

	localCache *common.RedisAdCampaignCache
}

func NewManager(r *common.RedisDB) *Manager {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	os.Setenv("broker", "10.0.0.131:9092")
	os.Setenv("group_id", "ad-golang1")
	os.Setenv("reset_offsets", "false")
	os.Setenv("events_topic", "ad-events")
	os.Setenv("workers", "5")
	os.Setenv("restart_interval", "5m")
	os.Setenv("log_interval", "1m")
	eventsTopic := os.Getenv("events_topic")
	// init consumer
	brokers := []string{os.Getenv("broker")}
	fmt.Println(brokers[0])
	topics := []string{eventsTopic}
	c, err := cluster.NewConsumer(brokers, os.Getenv("group_id"), topics, config)
	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(fmt.Errorf("Failed to create Consumer: %v", err))
	}
	cp := common.NewCampaignProcessor(r)
	if nil == cp {
		panic(fmt.Errorf("Failed to create Processor: %v", err))
	}
	workers, err := strconv.Atoi(os.Getenv("workers"))
	if err != nil {
		panic(fmt.Errorf("Error parsing $workers: %s", err.Error()))
	}

	restartInterval, err := time.ParseDuration(os.Getenv("restart_interval"))
	if err != nil {
		panic(fmt.Errorf("Error parsing $restart_interval: %s", err.Error()))
	}
	
	logInterval, err := time.ParseDuration(os.Getenv("log_interval"))
	if err != nil {
		panic(fmt.Errorf("Error parsing $log_interval: %s", err.Error()))
	}

	lc := common.NewRedisAdCampaignCache(r)

	return &Manager{
		c,
		cp,
		eventsTopic,
		int(workers),
		restartInterval,
		logInterval,
		r,
		make(chan *model.MetricEvent, 10000),
		make(chan *model.AdEvent),
		make(chan *model.AdEvent),
		make(chan *model.ProjectedEvent),
		lc,
	}
}

func (m *Manager) Restart() {
	// m.Stop() is not called here because we don't want to close(m.Metrics) during a restart
	m.consumer.Close()
	m.Start()
}

func (m *Manager) Start() {
	for i := 0; i < m.workers; i++ {
		go m.processor.Prepare()
		go m.startMetricConsumer(m.metrics)
		go m.startEnrichment(m.enrich)
		go m.startFilter(m.filter)
	}
	go m.startErrorConsumer(m.consumer.Errors())
	go m.startMessageConsumer(m.consumer.Messages())
	
}

func (m *Manager) Stop() {
	m.consumer.Close()
	defer close(m.metrics)
}

func (m *Manager) startErrorConsumer(in <-chan error) {
	for {
		select {
		case msg, ok := <-in:
			if ok {
				log.Printf("Error consuming message: %v", msg)
			} else {
				log.Print("Inbound ConsumerError channel has closed, terminating logger")
				in = nil
				return
			}
		}
	}
}

func (m *Manager) startMessageConsumer(in <-chan *sarama.ConsumerMessage) {
	restartTicker := time.NewTicker(m.restartInterval)
	logTicker := time.NewTicker(m.logInterval)
	adEventCount, msgCount := int64(0), int64(0)
	for {
		select {
		case msg, ok := <-in:
			if ok {
				msgCount++
				
				// handle the message
				switch msg.Topic {
				case m.eventsTopic:
					//fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
					adEventCount++
					go m.manageAdEvent(msg.Value)
				default:
					log.Fatalf("Unexpected message recieved on topic %s: %v", msg.Topic, string(msg.Value))
				}
				// mark message as processed
				m.consumer.MarkOffset(msg, "")

			} else {
				log.Println("Inbound ConsumerMessage channel has closed, terminating manager")
				in = nil
				return
			}
		case <- logTicker.C:
			log.Printf("AdEvent Count in the last one minute %d", adEventCount)
			adEventCount = int64(0)
		case <-restartTicker.C:
			if msgCount == 0 {
				log.Fatalf("Kafka Consumer received 0 messages during the previous %s: restarting consumer", m.restartInterval.String())
				m.Restart()
			} else {
				msgCount = int64(0)
			}
		}
	}
}

func (m *Manager) startMetricConsumer(in chan *model.MetricEvent) {
	for {
		select {
		case metric, ok := <-in:
			if ok {
				m.processor.Execute(metric.CampaignId, metric.Metric.Timestamp)
			} else {
				log.Println("Inbound metric channel has closed, terminating manager")
				in = nil
				return
			}
		}
	}
}



func (m *Manager) startFilter(in <-chan *model.AdEvent) {
	for {
		select {
		case adEvent, ok := <-in:
			if ok {
				m.filterEvent(adEvent)
			} else {
				log.Print("Inbound ConsumerError channel has closed, terminating logger")
				in = nil
				return
			}
		}
	}
}

func (m *Manager) startEnrichment(in <-chan *model.ProjectedEvent) {
	for {
		select {
		case projectedEvent, ok := <-in:
			if ok {
				m.enrichEvent(projectedEvent)
			} else {
				log.Print("Inbound ConsumerError channel has closed, terminating logger")
				in = nil
				return
			}
		}
	}
}

func (m *Manager) manageAdEvent(msg []byte) {
	//Parse the String as JSON
	adEvent := &model.AdEvent{}
	err := adEvent.ParseJson(msg)
	if err != nil {
		log.Fatalf("Error Unmarshalling payload: %v", err)
		return
	}
	m.filter <- adEvent
}

func (m *Manager) filterEvent(adEvent *model.AdEvent) {
	if adEvent.EventType == "view" {
		m.enrich <- &model.ProjectedEvent{fmt.Sprintf("%d", adEvent.EventTime), adEvent.AdId}
	}
}

//lookup in local cache and retrieve from redis
func (m *Manager) enrichEvent(projectedEvent *model.ProjectedEvent) {
	CampaignId := m.localCache.Execute(projectedEvent.AdId)
	if CampaignId != "" {
		enrichedEvent := &model.MetricEvent{CampaignId:CampaignId, Metric:&common.Window{Timestamp:projectedEvent.EventTime,SeenCount:0}}
		m.metrics <- enrichedEvent
	}
}