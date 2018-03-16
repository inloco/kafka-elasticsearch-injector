package fixtures

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/inloco/kafka-elasticsearch-injector/src/schema_registry"
	"github.com/Shopify/sarama"
)

const magic byte = 0

type KafkaEvent interface {
	Topic() string
	Schema() string
	ToAvroSerialization() ([]byte, error) // should return the avro serialized object
}

type Producer interface {
	Publish(KafkaEvent) error
	Start()
	Close() error
	GetSuccesses() <-chan *sarama.ProducerMessage
}
type FixtureSchemaRegistry struct {
	*schema_registry.SchemaRegistry
}

type AsyncProducer struct {
	kafka          sarama.AsyncProducer
	config         *sarama.Config
	schemaRegistry FixtureSchemaRegistry
	schemaHeaders  *sync.Map
}

/*
Config example :
	config.Producer.Return.Successes = true
	config.Producer.MaxMessageBytes = 20 * 1024 * 1024 // 20mb
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Version = sarama.V0_10_0_0                  // This version is the same as in production
*/
func NewProducer(kafkaAddr string, config *sarama.Config, schemaRegistry *schema_registry.SchemaRegistry) (Producer, error) {
	sarama.MaxRequestSize = 20 * 1024 * 1024 // 20mb
	client, err := sarama.NewAsyncProducer([]string{kafkaAddr}, config)
	if err != nil {
		return nil, err
	}

	return &AsyncProducer{client, config, FixtureSchemaRegistry{schemaRegistry}, &sync.Map{}}, nil
}

func (this *AsyncProducer) Start() {
	for {
		select {
		case err := <-this.kafka.Errors():
			fmt.Println("failed to publish event to kafka: ", err.Error())
		case x := <-this.kafka.Successes():
			fmt.Println(x)
		}
	}
}

func (this *AsyncProducer) GetSuccesses() <-chan *sarama.ProducerMessage {
	return this.kafka.Successes()
}

func (this *AsyncProducer) Close() error {
	return this.kafka.Close()
}

func (this *AsyncProducer) Publish(event KafkaEvent) error {

	header, err := this.GetHeader(event)
	if err != nil {
		return err
	}

	msg, err := event.ToAvroSerialization()
	if err != nil {
		return err
	}

	msg = append(header, msg...)

	this.kafka.Input() <- &sarama.ProducerMessage{
		Topic: event.Topic(),
		Value: sarama.ByteEncoder(msg),
	}
	return nil
}

func (this *AsyncProducer) GetHeader(event KafkaEvent) ([]byte, error) {
	cached, exists := this.schemaHeaders.Load(event.Topic())
	if !exists {
		id, err := this.schemaRegistry.RegisterOrGetSchemaId(event)
		if err != nil {
			return nil, err
		}

		binaryId := make([]byte, 4)
		binary.BigEndian.PutUint32(binaryId, uint32(id))

		header := make([]byte, 5)
		header[0] = magic
		for i := 1; i < 5; i++ {
			header[i] = binaryId[i-1]
		}

		this.schemaHeaders.Store(event.Topic(), header)
		return header, nil
	} else {
		return cached.([]byte), nil
	}
}

func (this *FixtureSchemaRegistry) RegisterOrGetSchemaId(event KafkaEvent) (int, error) {
	subject := event.Topic() + "-value"
	schema := event.Schema()
	fmt.Println("checking if schema is already registered", subject)
	registered, s, err := this.Client.IsRegistered(subject, schema)
	if err != nil {
		if strings.Contains(err.Error(), "40401") { // 40401 is the code returned by schema registry when the subject doesn't exists
			fmt.Println("creating subject and registering new version of schema", subject)
			return this.Client.RegisterNewSchema(subject, schema)
		}
		return 0, errors.New("schema registry (is registered) failure: " + err.Error())
	}

	if registered {
		fmt.Println(fmt.Sprintf("schema is registered: %d", s.Id))
		return s.Id, nil
	}

	fmt.Println("registering new version of schema", subject)

	id, err := this.Client.RegisterNewSchema(subject, schema)
	if err != nil {
		return 0, errors.New("schema registry (register schema) failure: " + err.Error())
	}

	return id, nil
}
