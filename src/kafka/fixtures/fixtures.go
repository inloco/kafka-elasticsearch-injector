package fixtures

import (
	"time"

	"math/rand"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka"
)

const DefaultTopic = "my-topic"

type FixtureRecord struct {
	Id string
}

func NewRecord(ts time.Time) (*kafka.Record, string) {
	id := string(rand.Int31())
	return &kafka.Record{
		Topic:     DefaultTopic,
		Partition: rand.Int31(),
		Offset:    rand.Int63(),
		Timestamp: ts,
		Json:      map[string]string{"id": id},
	}, id
}
