package kafka

import (
	"context"

	"encoding/json"

	"time"

	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/schema_registry"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

type Record struct {
	SchemaId  int32
	Topic     string
	Timestamp time.Time
	Json      []byte
	Avro      []byte
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
	jsonBytes, err := json.Marshal(native)
	if err != nil {
		return nil, err
	}

	return &Record{
		SchemaId:  schemaId,
		Topic:     msg.Topic,
		Timestamp: msg.Timestamp,
		Json:      jsonBytes,
		Avro:      avroRecord,
	}, nil
}

func getSchemaId(msg *sarama.ConsumerMessage) int32 {
	schemaIdBytes := msg.Value[1:5]
	return int32(schemaIdBytes[0])<<24 | int32(schemaIdBytes[1])<<16 | int32(schemaIdBytes[2])<<8 | int32(schemaIdBytes[3])
}
