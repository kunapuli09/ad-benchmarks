package core

import (
	"fmt"
	"log"
	"time"

	"os"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/kunapuli09/ad-benchmarks/common"
	"github.com/kunapuli09/ad-benchmarks/model"
	"github.com/wvanbergen/kafka/consumergroup"
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
	consumer        *consumergroup.ConsumerGroup
	processor       *common.CampaignProcessor
	eventsTopic     string
	workers         int
	flushInterval   time.Duration
	restartInterval time.Duration
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
	kafkaConsumerGroup := consumergroup.NewConfig()
	kafkaConsumerGroup.ClientID = "advertising-golang"
	kafkaConsumerGroup.Consumer.MaxProcessingTime = 1 * time.Second
	kafkaConsumerGroup.Offsets.Initial = sarama.OffsetNewest
	kafkaConsumerGroup.ChannelBufferSize = 10000
	kafkaConsumerGroup.Offsets.CommitInterval = 10 * time.Second
	kafkaConsumerGroup.Offsets.ResetOffsets = os.Getenv("reset_offsets") == "true"

	eventsTopic := os.Getenv("events_topic")

	cg, err := consumergroup.JoinConsumerGroup(os.Getenv("group_id"), []string{eventsTopic}, strings.Split(os.Getenv("zookeepers"), ","), kafkaConsumerGroup)
	if err != nil {
		panic(fmt.Errorf("Failed to create ConsumerGroup: %v", err))
	}
	cp := common.NewCampaignProcessor(r)
	workers, err := strconv.Atoi(os.Getenv("workers"))
	if err != nil {
		panic(fmt.Errorf("Error parsing $workers: %s", err.Error()))
	}

	flushInterval, err := time.ParseDuration(os.Getenv("flush_interval"))
	if err != nil {
		panic(fmt.Errorf("Error parsing $flush_interval: %s", err.Error()))
	}

	restartInterval, err := time.ParseDuration(os.Getenv("restart_interval"))
	if err != nil {
		panic(fmt.Errorf("Error parsing $restart_interval: %s", err.Error()))
	}

	lc := common.NewRedisAdCampaignCache(r)

	return &Manager{
		cg,
		cp,
		eventsTopic,
		int(workers),
		flushInterval,
		restartInterval,
		r,
		make(chan *model.MetricEvent, 10000),
		make(chan *model.AdEvent),
		make(chan *model.AdEvent),
		make(chan *model.ProjectedEvent),
		lc,
	}
}

func (m *Manager) Restart() {
	// dont call m.Stop() here because we don't want to close(m.points) during a restart
	m.consumer.Close()
	m.Start()
}

func (m *Manager) Start() {
	for i := 0; i < m.workers; i++ {
		go m.startMetricConsumer(m.metrics)
		go m.startFilter(m.filter)
		go m.startEnrichment(m.enrich)
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
	msgCount := int64(0)
	offsets := make(map[string]map[int32]int64)
	for {
		select {
		case msg, ok := <-in:
			if ok {
				msgCount++

				// track topic/partition offsets so we can warn if things go awry
				if offsets[msg.Topic] == nil {
					offsets[msg.Topic] = make(map[int32]int64)
				}
				if offsets[msg.Topic][msg.Partition] != 0 && offsets[msg.Topic][msg.Partition] != msg.Offset-1 {
					log.Printf("Unexpected offset on %s:%d: Expected %d, Found %d (diff: %d)", msg.Topic, msg.Partition, offsets[msg.Topic][msg.Partition]+1, msg.Offset, msg.Offset-offsets[msg.Topic][msg.Partition]+1)
				}

				// handle the message
				switch msg.Topic {
				case m.eventsTopic:
					go m.manageAdEvent(msg.Value)
				default:
					log.Fatalf("Unexpected message recieved on topic %s: %v", msg.Topic, string(msg.Value))
				}

				// assume the message was processed successfully. m is good-enough(tm)
				offsets[msg.Topic][msg.Partition] = msg.Offset
				m.consumer.CommitUpto(msg)

			} else {
				log.Printf("Inbound ConsumerMessage channel has closed, terminating manager")
				in = nil
				return
			}
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
				log.Printf("Inbound metric channel has closed, terminating manager")
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
		m.enrich <- &model.ProjectedEvent{adEvent.EventTime, adEvent.AdId}
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
