package fixtures

import (
	"time"

	"math/rand"

	"github.com/inloco/goavro"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
)

const DefaultTopic = "my-topic"

type FixtureRecord struct {
	Id        int32
	Value     int32
	Timestamp int64 `json:"@timestamp"`
}

func (r *FixtureRecord) Topic() string {
	return DefaultTopic
}

func (r *FixtureRecord) Schema() string {
	return `{"type": "record","name": "FixtureRecord","fields": [{"name": "id","type":"int"}]}`
}

func (r *FixtureRecord) ToAvroSerialization() ([]byte, error) {
	codec, err := goavro.NewCodec(r.Schema())
	if err != nil {
		return []byte{}, nil
	}
	return codec.BinaryFromNative(nil, map[string]interface{}{"id": r.Id})
}

func NewFixtureRecord() *FixtureRecord {
	return &FixtureRecord{Id: rand.Int31()}
}

func NewRecord(ts time.Time) (*models.Record, int32, int32) {
	id := rand.Int31()
	value := rand.Int31()
	return &models.Record{
		Topic:     DefaultTopic,
		Partition: rand.Int31(),
		Offset:    rand.Int63(),
		Timestamp: ts,
		Json:      map[string]interface{}{"id": id, "value": value},
	}, id, value
}
