package fixtures

import (
	"time"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka"
)

const DefaultTopic = "my-topic"

type FixtureRecord struct {
	id string
}

func NewRecord(ts time.Time) *kafka.Record {
	return &kafka.Record{
		Topic:     DefaultTopic,
		Timestamp: ts,
		Json:      map[string]string{"id": "xalala"},
	}
}
