package main

import (
	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/log/level"
	"github.com/inloco/kafka-elasticsearch-injector/src/kafka/fixtures"
	"github.com/inloco/kafka-elasticsearch-injector/src/logger_builder"
	"github.com/inloco/kafka-elasticsearch-injector/src/schema_registry"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	logger := logger_builder.NewLogger("test-producer")
	registry, err := schema_registry.NewSchemaRegistry(os.Getenv("SCHEMA_REGISTRY_URL"))
	if err != nil {
		panic(err)
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.MaxMessageBytes = 20 * 1024 * 1024 // 20mb
	config.Producer.Flush.Frequency = 1 * time.Millisecond
	config.Version = sarama.V0_10_0_0 // This version is the same as in production
	producer, err := fixtures.NewProducer(os.Getenv("KAFKA_ADDRESS"), config, registry)
	if err != nil {
		panic(err)
	}
	numProducers := 1
	for i := 0; i < numProducers; i++ {
		go func() {
			for {
				rec := fixtures.NewFixtureRecord()
				err := producer.Publish(rec)
				if err != nil {
					level.Error(logger).Log("msg", "error publishing to kafka", "err", err)
					continue
				}
				msg := <-producer.GetSuccesses()
				level.Debug(logger).Log("msg", "produced!", "value", rec, "msg", *msg)
			}
		}()
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signals:
			return
		}
	}
}
