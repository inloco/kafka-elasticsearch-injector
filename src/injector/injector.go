package injector

import (
	"strconv"

	"time"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/schema_registry"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
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
	bufferWaitTime, err := time.ParseDuration(kafkaConfig.BufferWaitTime)
	if err != nil {
		level.Warn(logger).Log("err", err, "message", "failed to get consumer buffer wait time")
		bufferWaitTime = 1 * time.Second
	}
	deserializer := &kafka.Decoder{
		SchemaRegistry: schemaRegistry,
	}

	return kafka.Consumer{
		Topics:                kafkaConfig.Topics,
		Group:                 kafkaConfig.ConsumerGroup,
		Endpoint:              endpoints.Insert(),
		Decoder:               deserializer.KafkaMessageToRecord,
		Logger:                logger,
		Concurrency:           concurrency,
		BatchSize:             batchSize,
		MetricsUpdateInterval: metricsUpdateInterval,
		BufferWaitTime:        bufferWaitTime,
	}, nil
}
