package kafka

import (
	"testing"

	"os"
	"time"

	"context"

	"fmt"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/elasticsearch"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka/fixtures"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/logger_builder"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/models"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/schema_registry"
	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/endpoint"
	"github.com/stretchr/testify/assert"
)

type fixtureService struct {
	db elasticsearch.RecordDatabase
}

func (s fixtureService) Insert(records []*models.Record) error {
	return s.db.Insert(records)
}

func (s fixtureService) ReadinessCheck() bool {
	return s.db.ReadinessCheck()
}

type fixtureEndpoints struct {
	insertEndpoint endpoint.Endpoint
}

func (be *fixtureEndpoints) Insert() endpoint.Endpoint {
	return be.insertEndpoint
}

var (
	logger = logger_builder.NewLogger("consumer-test")
	db     = elasticsearch.NewDatabase(logger, elasticsearch.Config{
		Host:        "http://localhost:9200",
		Index:       fixtures.DefaultTopic,
		BulkTimeout: 10 * time.Second,
	})
	service   = fixtureService{db}
	endpoints = &fixtureEndpoints{
		func(ctx context.Context, request interface{}) (response interface{}, err error) {
			records := request.([]*models.Record)

			return nil, service.Insert(records)
		},
	}
	kafkaConfig = &Config{
		Type:          KafkaConsumer,
		Topics:        []string{fixtures.DefaultTopic},
		ConsumerGroup: "my-consumer-group",
		Concurrency:   "1",
		BatchSize:     "1",
	}
	schemaRegistry *schema_registry.SchemaRegistry
	k              kafka
)

func TestMain(m *testing.M) {
	registry, err := schema_registry.NewSchemaRegistry("http://localhost:8081", &schema_registry.Schema{
		Type:    schema_registry.SchemaTypeValue,
		Version: "1",
		Subject: fixtures.DefaultTopic + "-value",
	})
	if err != nil {
		panic(err)
	}
	schemaRegistry = registry
	decoder := Decoder{
		SchemaRegistry: schemaRegistry,
	}
	consumer := Consumer{
		Topics:      kafkaConfig.Topics,
		Group:       kafkaConfig.ConsumerGroup,
		Endpoint:    endpoints.Insert(),
		Decoder:     decoder.KafkaMessageToRecord,
		Logger:      logger,
		Concurrency: 1,
	}
	k = NewKafka("localhost:9092", consumer)
	retCode := m.Run()
	os.Exit(retCode)
}

func TestKafka_Start(t *testing.T) {
	signals := make(chan os.Signal, 1)
	go k.Start(signals)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.MaxMessageBytes = 20 * 1024 * 1024 // 20mb
	config.Producer.Flush.Frequency = 1 * time.Millisecond
	config.Version = sarama.V0_10_0_0 // This version is the same as in production
	time.Sleep(10 * time.Second)
	producer, err := fixtures.NewProducer("localhost:9092", config, schemaRegistry)
	if assert.NoError(t, err) {
		rec := fixtures.NewFixtureRecord()

		err = producer.Publish(&rec)
		if !assert.NoError(t, err) {
			fmt.Println(err)
			fmt.Println("dsaoijd")
		}
		fmt.Println("success")
	}
	producer.Start()
	time.Sleep(100 * time.Second)
	signals <- os.Interrupt
}
