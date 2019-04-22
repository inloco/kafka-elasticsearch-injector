package kafka

import (
	"testing"

	"os"
	"time"

	"context"

	"fmt"

	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/endpoint"
	"github.com/inloco/kafka-elasticsearch-injector/src/elasticsearch"
	"github.com/inloco/kafka-elasticsearch-injector/src/kafka/fixtures"
	"github.com/inloco/kafka-elasticsearch-injector/src/logger_builder"
	"github.com/inloco/kafka-elasticsearch-injector/src/metrics"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
	"github.com/inloco/kafka-elasticsearch-injector/src/schema_registry"
	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

type fixtureService struct {
	db    elasticsearch.RecordDatabase
	codec elasticsearch.Codec
}

func (s fixtureService) Insert(records []*models.Record) error {
	elasticRecords, err := s.codec.EncodeElasticRecords(records)
	if err != nil {
		return err
	}
	_, err = s.db.Insert(elasticRecords)
	return err
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
	config = elasticsearch.Config{
		Host:        "http://localhost:9200",
		Index:       fixtures.DefaultTopic,
		BulkTimeout: 10 * time.Second,
	}
	db        = elasticsearch.NewDatabase(logger, config)
	codec     = elasticsearch.NewCodec(logger, config)
	service   = fixtureService{db, codec}
	endpoints = &fixtureEndpoints{
		func(ctx context.Context, request interface{}) (response interface{}, err error) {
			records := request.([]*models.Record)

			return nil, service.Insert(records)
		},
	}
	schemaRegistry *schema_registry.SchemaRegistry
	k              kafka
)

func TestMain(m *testing.M) {
	registry, err := schema_registry.NewSchemaRegistry("http://localhost:8081")
	if err != nil {
		panic(err)
	}
	schemaRegistry = registry
	decoder := Decoder{
		SchemaRegistry: schemaRegistry,
	}
	consumer := Consumer{
		Topics:                []string{fixtures.DefaultTopic},
		Group:                 "my-consumer-group",
		Endpoint:              endpoints.Insert(),
		Decoder:               decoder.AvroMessageToRecord,
		Logger:                logger,
		Concurrency:           1,
		BatchSize:             1,
		MetricsUpdateInterval: 30 * time.Second,
	}
	k = NewKafka("localhost:9092", consumer, metrics.NewMetricsPublisher())
	retCode := m.Run()
	os.Exit(retCode)
}

func TestKafka_Start(t *testing.T) {
	signals := make(chan os.Signal, 1)
	notifications := make(chan Notification, 1)
	go k.Start(signals, notifications)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.MaxMessageBytes = 20 * 1024 * 1024 // 20mb
	config.Producer.Flush.Frequency = 1 * time.Millisecond
	config.Version = sarama.V0_10_0_0 // This version is the same as in production
	<-notifications
	producer, err := fixtures.NewProducer("localhost:9092", config, schemaRegistry)
	expectedTimestamp := time.Now().UnixNano() / int64(time.Millisecond)
	rec := fixtures.NewFixtureRecord()
	var msg *sarama.ProducerMessage
	if assert.NoError(t, err) {

		err = producer.Publish(rec)
		if assert.NoError(t, err) {
			msg = <-producer.GetSuccesses()
		} else {
			fmt.Println(err.Error())
		}
	}
	<-notifications
	esIndex := fmt.Sprintf("%s-%s", msg.Topic, time.Now().Format("2006-01-02"))
	esId := fmt.Sprintf("%d:%d", msg.Partition, msg.Offset)
	_, err = db.GetClient().Refresh(esIndex).Do(context.Background())
	if assert.NoError(t, err) {
		res, err := db.GetClient().Get().Index(esIndex).Id(esId).Do(context.Background())
		var r fixtures.FixtureRecord
		if assert.NoError(t, err) {
			assert.True(t, res.Found)
			err = json.Unmarshal(res.Source, &r)
			if assert.NoError(t, err) {
				assert.Equal(t, rec.Id, r.Id)
				assert.InDelta(t, expectedTimestamp, r.Timestamp, 5000.0)
			}
		}
		signals <- os.Interrupt
	}
	db.GetClient().DeleteByQuery(esIndex).Query(elastic.MatchAllQuery{}).Do(context.Background())
	db.CloseClient()
}
