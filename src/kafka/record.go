package kafka

import (
	"context"

	"time"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/schema_registry"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

type Record struct {
	Topic     string
	Timestamp time.Time
	Json      interface{}
}

func (r *Record) FormatTimestamp() string {
	return r.Timestamp.Format("2006-01-02")
}

type Decoder struct {
	SchemaRegistry *schema_registry.SchemaRegistry
}

func (d *Decoder) KafkaMessageToRecord(context context.Context, msg *sarama.ConsumerMessage) (*Record, error) {
	schemaId := getSchemaId(msg)
	avroRecord := msg.Value[5:]
	schema, err := d.SchemaRegistry.GetSchema(msg.Topic, schemaId)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	native, _, err := codec.NativeFromBinary(avroRecord)
	if err != nil {
		return nil, err
	}

	return &Record{
		Topic:     msg.Topic,
		Timestamp: msg.Timestamp,
		Json:      native,
	}, nil
}

func getSchemaId(msg *sarama.ConsumerMessage) int32 {
	schemaIdBytes := msg.Value[1:5]
	return int32(schemaIdBytes[0])<<24 | int32(schemaIdBytes[1])<<16 | int32(schemaIdBytes[2])<<8 | int32(schemaIdBytes[3])
}
