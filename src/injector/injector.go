package injector

import (
	"strconv"

	"time"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/schema_registry"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

var recordTypes = map[string]func(d *kafka.Decoder) kafka.DecodeMessageFunc{
	"avro": func(d *kafka.Decoder) kafka.DecodeMessageFunc {
		return d.AvroMessageToRecord
	},
	"json": func(d *kafka.Decoder) kafka.DecodeMessageFunc {
		return d.JsonMessageToRecord
	},
}

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
	decodeFunc := deserializer.AvroMessageToRecord
	if fn, ok := recordTypes[kafkaConfig.RecordType]; ok {
		decodeFunc = fn(deserializer)
	}

	return kafka.Consumer{
		Topics:                kafkaConfig.Topics,
		Group:                 kafkaConfig.ConsumerGroup,
		Endpoint:              endpoints.Insert(),
		Decoder:               decodeFunc,
		Logger:                logger,
		Concurrency:           concurrency,
		BatchSize:             batchSize,
		MetricsUpdateInterval: metricsUpdateInterval,
		BufferSize:            bufferSize,
	}, nil
}
