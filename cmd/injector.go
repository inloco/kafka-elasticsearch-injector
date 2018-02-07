package main

import (
	"fmt"
	"os"

	"strings"

	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/injector"
	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/kafka"
	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/logger_builder"
	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/metrics_instrumenter"
	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/probes"
	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/schema_registry"
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
	schemaRegistry, err := schema_registry.NewSchemaRegistry(os.Getenv("SCHEMA_REGISTRY_URL"), &schema_registry.Schema{
		Type:    schema_registry.SchemaTypeValue,
		Version: os.Getenv("SCHEMA_REGISTRY_VISIT_VERSION"),
		Subject: os.Getenv("SCHEMA_REGISTRY_VISIT_SUBJECT"),
	})
	if err != nil {
		level.Error(logger).Log("err", err, "message", "failed to create schema registry client")
	}

	kafkaConfig := &kafka.Config{
		Type:          kafka.KafkaConsumer,
		Topics:        strings.Split(os.Getenv("KAFKA_VISITS_TOPIC"), ","),
		ConsumerGroup: os.Getenv("KAFKA_VISITS_CONSUMER_GROUP"),
		Concurrency:   os.Getenv("KAFKA_VISITS_CONSUMER_CONCURRENCY"),
		BatchSize:     os.Getenv("KAFKA_VISITS_CONSUMER_BATCH_SIZE"),
	}

	service := injector.NewService(logger)
	p.SetReadinessCheck(service.ReadinessCheck)

	endpoints := injector.Endpoints{
		Insert: injector.MakeInsertEndpoint(service),
	}

	consumer, err := injector.MakeKafkaConsumer(&endpoints, logger, schemaRegistry, kafkaConfig)
	if err != nil {
		level.Error(logger).Log("err", err, "message", "error creating kafka consumer")
		panic(err)
	}
	k := kafka.NewKafka()
	k.RegisterConsumer(consumer)

	k.Start()
}
