package fixtures

import (
	"time"

	"math/rand"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/models"
	"github.com/linkedin/goavro"
)

const DefaultTopic = "my-topic-22"

type FixtureRecord struct {
	Id string
}

func (r *FixtureRecord) Topic() string {
	return DefaultTopic
}

func (r *FixtureRecord) Schema() string {
	return `{"type": "record","name": "FixtureRecord","fields": [{"name": "id","type":"string"}]}`
}

func (r *FixtureRecord) ToAvroSerialization() ([]byte, error) {
	codec, err := goavro.NewCodec(r.Schema())
	if err != nil {
		return []byte{}, nil
	}
	return codec.BinaryFromNative(nil, map[string]interface{}{"id": r.Id})
}

func NewFixtureRecord() FixtureRecord {
	return FixtureRecord{Id: string(rand.Int31())}
}

func NewRecord(ts time.Time) (*models.Record, string) {
	id := string(rand.Int31())
	return &models.Record{
		Topic:     DefaultTopic,
		Partition: rand.Int31(),
		Offset:    rand.Int63(),
		Timestamp: ts,
		Json:      map[string]string{"id": id},
	}, id
}
