package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kunapuli09/ad-benchmarks/core"
	"github.com/kunapuli09/ad-benchmarks/common"
)

func main() {
	log.Print("Initialize go-benchmarks")
	os.Setenv("zookeepers", "10.0.0.131:2181")
	os.Setenv("group_id", "advertising-golang")
	os.Setenv("reset_offsets", "false")
	os.Setenv("events_topic", "ad-events")
	os.Setenv("workers", "5")
	os.Setenv("flush_interval", "500ms")
	os.Setenv("stats_interval", "1m")
	os.Setenv("restart_interval", "5m")
	os.Setenv("redis_urls", "http://localhost:6379")
	os.Setenv("redis_database", "ads")

	// manage graceful shutdown.
	signals := make(chan os.Signal, 1)
	stop := make(chan struct{})
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		signal := <-signals
		log.Printf("Received %v. Stopping", signal)
		close(stop)
	}()

	// redis
	redis := common.NewRedisDB()
	manager := core.NewManager(redis)

	//life-cycle start
	manager.Start()
	log.Printf("Started go-benchmarks")
	//life-cycle shutdown
	<-stop
	log.Printf("Shutting down go-benchmarks")
	manager.Stop()
	log.Printf("Stopped go-benchmarks")
}
