package injector

import (
	"strconv"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/schema_registry"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

func MakeKafkaConsumer(endpoints Endpoints, logger log.Logger, schemaRegistry *schema_registry.SchemaRegistry, kafkaConfig *kafka.Config) (kafka.Consumer, error) {

	err := schemaRegistry.ValidateSchema(logger)
	if err != nil {
		return kafka.Consumer{}, err
	}

	concurrency, err := strconv.Atoi(kafkaConfig.Concurrency)
	if err != nil {
		level.Warn(logger).Log("err", err, "message", "failed to get consumer concurrency")
		concurrency = 1
	}
	deserializer := &kafka.Decoder{
		SchemaRegistry: schemaRegistry,
	}

	return kafka.Consumer{
		Topics:      kafkaConfig.Topics,
		Group:       kafkaConfig.ConsumerGroup,
		Endpoint:    endpoints.Insert(),
		Decoder:     deserializer.KafkaMessageToRecord,
		Logger:      logger,
		Concurrency: concurrency,
	}, nil
}
