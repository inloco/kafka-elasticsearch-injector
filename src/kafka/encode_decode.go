package kafka

import (
	"context"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/models"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/schema_registry"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

// DecodeMessageFunc extracts a user-domain request object from an Kafka
// message object. It's designed to be used in Kafka consumers.
// One straightforward DecodeMessageFunc could be something that
// Avro decodes the message body to the concrete response type.
type DecodeMessageFunc func(context.Context, *sarama.ConsumerMessage) (record *models.Record, err error)

type Decoder struct {
	SchemaRegistry *schema_registry.SchemaRegistry
}

func (d *Decoder) KafkaMessageToRecord(context context.Context, msg *sarama.ConsumerMessage) (*models.Record, error) {
	schemaId := getSchemaId(msg)
	avroRecord := msg.Value[5:]
	schema, err := d.SchemaRegistry.GetSchema(msg.Topic+"-value", schemaId)
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

	return &models.Record{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
		Json:      native,
	}, nil
}

func getSchemaId(msg *sarama.ConsumerMessage) int32 {
	schemaIdBytes := msg.Value[1:5]
	return int32(schemaIdBytes[0])<<24 | int32(schemaIdBytes[1])<<16 | int32(schemaIdBytes[2])<<8 | int32(schemaIdBytes[3])
}
