package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	e "github.com/inloco/kafka-elasticsearch-injector/src/errors"
)

type dummy struct {
	Id        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
}

func TestDecoder_JsonMessageToRecord(t *testing.T) {
	d := &Decoder{CodecCache: sync.Map{}}
	val := dummy{"alo", 60}
	jsonBytes, err := json.Marshal(val)
	record, err := d.JsonMessageToRecord(context.Background(), &sarama.ConsumerMessage{
		Value:     jsonBytes,
		Topic:     "test",
		Partition: 1,
		Offset:    54,
		Timestamp: time.Now(),
	})
	assert.Nil(t, err)
	returnedJsonBytes, err := json.Marshal(record.Json)
	assert.Nil(t, err)
	var returnedVal dummy
	err = json.Unmarshal(returnedJsonBytes, &returnedVal)
	assert.Nil(t, err)
	assert.Equal(t, val, returnedVal)
}

func TestDecoder_JsonMessageToRecord_MalformedJson(t *testing.T) {
	d := &Decoder{CodecCache: sync.Map{}}
	jsonBytes := []byte(`{"alo": 60"`)
	record, err := d.JsonMessageToRecord(context.Background(), &sarama.ConsumerMessage{
		Value:     jsonBytes,
		Topic:     "test",
		Partition: 1,
		Offset:    54,
		Timestamp: time.Now(),
	})
	assert.Nil(t, record)
	assert.NotNil(t, err)
}

func TestDecoder_AvroMessageToRecord_NilMessageValue(t *testing.T) {
	d := &Decoder{CodecCache: sync.Map{}}
	record, err := d.AvroMessageToRecord(nil, &sarama.ConsumerMessage{Value: nil})
	isErrNilMessage := errors.Is(err, e.ErrNilMessage)
	assert.Nil(t, record)
	assert.Equal(t, isErrNilMessage, true)
}
