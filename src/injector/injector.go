package injector

import (
	"strconv"

	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/inloco/kafka-elasticsearch-injector/src/kafka"
	"github.com/inloco/kafka-elasticsearch-injector/src/schema_registry"
)

func MakeKafkaConsumer(endpoints Endpoints, logger log.Logger, schemaRegistry *schema_registry.SchemaRegistry, kafkaConfig *kafka.Config) (kafka.Consumer, error) {
	concurrency, err := strconv.Atoi(kafkaConfig.Concurrency)
	if err != nil {
		level.Warn(logger).Log("err", err, "message", "failed to get consumer concurrency")
		concurrency = 1
	}
	batchSize, err := strconv.Atoi(kafkaConfig.BatchSize)
	if err != nil {
		level.Warn(logger).Log("err", err, "message", "failed to get consumer batch size")
		batchSize = 100
	}
	metricsUpdateInterval, err := time.ParseDuration(kafkaConfig.MetricsUpdateInterval)
	if err != nil {
		level.Warn(logger).Log("err", err, "message", "failed to get consumer metrics update interval")
		metricsUpdateInterval = 30 * time.Second
	}

	bufferSize, err := strconv.Atoi(kafkaConfig.BufferSize)
	if err != nil {
		bufferSize = batchSize * concurrency
	}

	deserializer := &kafka.Decoder{
		SchemaRegistry: schemaRegistry,
	}

	includeKey, err := strconv.ParseBool(kafkaConfig.IncludeKey)
	if err != nil {
		level.Warn(logger).Log("err", err, "message", "failed to get consumer with key and value")
		includeKey = false
	}

	return kafka.Consumer{
		Topics:                kafkaConfig.Topics,
		Group:                 kafkaConfig.ConsumerGroup,
		Endpoint:              endpoints.Insert(),
		Decoder:               deserializer.DeserializerFor(kafkaConfig.RecordType),
		Logger:                logger,
		Concurrency:           concurrency,
		BatchSize:             batchSize,
		MetricsUpdateInterval: metricsUpdateInterval,
		BufferSize:            bufferSize,
		IncludeKey:            includeKey,
	}, nil
}
