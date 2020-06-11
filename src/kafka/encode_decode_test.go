package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	e "github.com/inloco/kafka-elasticsearch-injector/src/errors"
	"github.com/stretchr/testify/assert"
)

type dummyValue struct {
	Id        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
}

type dummyKey struct {
	Id string `json:"id"`
}

type dummyIncludeKey struct {
	Id        string   `json:"id"`
	Timestamp int64    `json:"timestamp"`
	Key       dummyKey `json:"key"`
}

func TestDecoder_JsonMessageToRecord(t *testing.T) {
	d := &Decoder{CodecCache: sync.Map{}}
	val := dummyValue{"alo", 60}
	jsonBytes, err := json.Marshal(val)
	record, err := d.JsonMessageToRecord(context.Background(), &sarama.ConsumerMessage{
		Value:     jsonBytes,
		Topic:     "test",
		Partition: 1,
		Offset:    54,
		Timestamp: time.Now(),
	}, false)
	assert.Nil(t, err)
	returnedJsonBytes, err := json.Marshal(record.Json)
	assert.Nil(t, err)
	var returnedVal dummyValue
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
	}, false)
	assert.Nil(t, record)
	assert.NotNil(t, err)
}

func TestDecoder_AvroMessageToRecord_NilMessageValue(t *testing.T) {
	d := &Decoder{CodecCache: sync.Map{}}
	record, err := d.AvroMessageToRecord(context.Background(), &sarama.ConsumerMessage{
		Value:     nil,
		Topic:     "test",
		Partition: 1, Offset: 54,
		Timestamp: time.Now()},
		false)
	isErrNilMessage := errors.Is(err, e.ErrNilMessage)
	assert.Nil(t, record)
	assert.True(t, isErrNilMessage)
}

func TestDecoder_AvroMessageToRecord_NilMessageValue_IncludeKey(t *testing.T) {
	d := &Decoder{CodecCache: sync.Map{}}
	key := dummyKey{"marco"}
	jsonBytesKey, _ := json.Marshal(key)
	record, err := d.AvroMessageToRecord(context.Background(), &sarama.ConsumerMessage{
		Value:     nil,
		Key:       jsonBytesKey,
		Topic:     "test",
		Partition: 1, Offset: 54,
		Timestamp: time.Now()},
		true)
	isErrNilMessage := errors.Is(err, e.ErrNilMessage)
	assert.Nil(t, record)
	assert.True(t, isErrNilMessage)
}

func TestDecoder_JsonMessageToRecord_IncludeKey(t *testing.T) {
	d := &Decoder{CodecCache: sync.Map{}}

	key := dummyKey{"marco"}
	jsonBytesKey, _ := json.Marshal(key)

	val := dummyValue{"pop", 60}
	jsonBytesValue, _ := json.Marshal(val)

	expected := dummyIncludeKey{"pop", 60, dummyKey{"marco"}}

	record, err := d.JsonMessageToRecord(context.Background(), &sarama.ConsumerMessage{
		Key:       jsonBytesKey,
		Value:     jsonBytesValue,
		Topic:     "test",
		Partition: 1,
		Offset:    54,
		Timestamp: time.Now(),
	}, true)
	assert.Nil(t, err)

	returnedJsonBytes, err := json.Marshal(record.Json)
	assert.Nil(t, err)

	var returnedVal dummyValue
	err = json.Unmarshal(returnedJsonBytes, &returnedVal)
	assert.Nil(t, err)
	assert.Equal(t, val, returnedVal)

	var returnedKeyIncluded dummyIncludeKey
	err = json.Unmarshal(returnedJsonBytes, &returnedKeyIncluded)
	assert.Nil(t, err)
	assert.Equal(t, expected, returnedKeyIncluded)
}
