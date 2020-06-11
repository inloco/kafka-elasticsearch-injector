package main

import (
	"fmt"
	"os"

	"strings"

	"os/signal"
	"syscall"

	"github.com/go-kit/kit/log/level"
	"github.com/inloco/kafka-elasticsearch-injector/src/injector"
	"github.com/inloco/kafka-elasticsearch-injector/src/kafka"
	"github.com/inloco/kafka-elasticsearch-injector/src/logger_builder"
	"github.com/inloco/kafka-elasticsearch-injector/src/metrics"
	"github.com/inloco/kafka-elasticsearch-injector/src/probes"
	"github.com/inloco/kafka-elasticsearch-injector/src/schema_registry"
)

func main() {
	logger := logger_builder.NewLogger("kafka-elasticsearch-injector")

	probesPort := os.Getenv("PROBES_PORT")
	p := probes.New(probesPort)
	p.SetLivenessCheck(func() bool {
		return true
	})
	level.Info(logger).Log(
		"message", fmt.Sprintf("Initializing kubernetes probes at %s", probesPort),
	)
	go p.Serve()
	metrics.Register()
	schemaRegistry, err := schema_registry.NewSchemaRegistry(os.Getenv("SCHEMA_REGISTRY_URL"))
	if err != nil {
		level.Error(logger).Log("err", err, "message", "failed to create schema registry client")
	}

	kafkaConfig := &kafka.Config{
		Type:                  kafka.ConsumerType,
		Topics:                strings.Split(os.Getenv("KAFKA_TOPICS"), ","),
		ConsumerGroup:         os.Getenv("KAFKA_CONSUMER_GROUP"),
		Concurrency:           os.Getenv("KAFKA_CONSUMER_CONCURRENCY"),
		BatchSize:             os.Getenv("KAFKA_CONSUMER_BATCH_SIZE"),
		BufferSize:            os.Getenv("KAFKA_CONSUMER_BUFFER_SIZE"),
		MetricsUpdateInterval: os.Getenv("KAFKA_CONSUMER_METRICS_UPDATE_INTERVAL"),
		RecordType:            os.Getenv("KAFKA_CONSUMER_RECORD_TYPE"),
		IncludeKey:            os.Getenv("KAFKA_CONSUMER_INCLUDE_KEY"),
	}
	metricsPublisher := metrics.NewMetricsPublisher()
	service := injector.NewService(logger, metricsPublisher)
	p.SetReadinessCheck(service.ReadinessCheck)

	endpoints := injector.MakeEndpoints(service)

	consumer, err := injector.MakeKafkaConsumer(endpoints, logger, schemaRegistry, kafkaConfig)
	if err != nil {
		level.Error(logger).Log("err", err, "message", "error creating kafka consumer")
		panic(err)
	}
	k := kafka.NewKafka(os.Getenv("KAFKA_ADDRESS"), consumer, metricsPublisher)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	notifications := make(chan kafka.Notification, 10)
	go func() {
		for {
			ntf := <-notifications
			switch ntf {
			case kafka.Ready:
				level.Info(logger).Log("message", "kafka consumer ready")
			case kafka.Inserted:
				level.Info(logger).Log("message", fmt.Sprintf("inserted records"))
			}
		}
	}()
	k.Start(signals, notifications)
}
