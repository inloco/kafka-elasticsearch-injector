package kafka

import (
	"context"

	"github.com/Shopify/sarama"
)

// DecodeMessageFunc extracts a user-domain request object from an Kafka
// message object. It's designed to be used in Kafka consumers.
// One straightforward DecodeMessageFunc could be something that
// Avro decodes the message body to the concrete response type.
type DecodeMessageFunc func(context.Context, *sarama.ConsumerMessage) (record *Record, err error)
