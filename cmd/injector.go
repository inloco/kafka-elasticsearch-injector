package main

import (
	"fmt"
	"os"

	"strings"

	"os/signal"
	"syscall"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/injector"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/logger_builder"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/metrics_instrumenter"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/probes"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/schema_registry"
	"github.com/go-kit/kit/log/level"
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
	metrics_instrumenter.Register()
	schemaRegistry, err := schema_registry.NewSchemaRegistry(os.Getenv("SCHEMA_REGISTRY_URL"))
	if err != nil {
		level.Error(logger).Log("err", err, "message", "failed to create schema registry client")
	}

	kafkaConfig := &kafka.Config{
		Type:          kafka.KafkaConsumer,
		Topics:        strings.Split(os.Getenv("KAFKA_TOPICS"), ","),
		ConsumerGroup: os.Getenv("KAFKA_CONSUMER_GROUP"),
		Concurrency:   os.Getenv("KAFKA_CONSUMER_CONCURRENCY"),
		BatchSize:     os.Getenv("KAFKA_CONSUMER_BATCH_SIZE"),
	}

	service := injector.NewService(logger)
	p.SetReadinessCheck(service.ReadinessCheck)

	endpoints := injector.MakeEndpoints(service)

	consumer, err := injector.MakeKafkaConsumer(endpoints, logger, schemaRegistry, kafkaConfig)
	if err != nil {
		level.Error(logger).Log("err", err, "message", "error creating kafka consumer")
		panic(err)
	}
	k := kafka.NewKafka(os.Getenv("KAFKA_ADDRESS"), consumer)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	notifications := make(chan kafka.Notification, 10)
	go func() {
		for {
			select {
			case _ = <-notifications: //ignore notifications
			}
		}
	}()
	k.Start(signals, notifications)
}
