package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kunapuli09/ad-benchmarks/core"
	"github.com/kunapuli09/ad-benchmarks/common"
	//"runtime"
)

func main() {
	////this setting is for a 2CPU machine (Ex: mac book pro 2016 with touch bar)
	//runtime.GOMAXPROCS(2)
	log.Print("Initialize go-benchmarks")

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
	log.Printf("Started ad-benchmarks")
	//life-cycle shutdown
	<-stop
	log.Printf("Shutting down ad-benchmarks")
	manager.Stop()
	log.Printf("Stopped ad-benchmarks")
}
