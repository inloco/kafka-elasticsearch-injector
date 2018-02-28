package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

type dummy struct {
	Id        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
}

func TestDecoder_JsonMessageToRecord(t *testing.T) {
	d := &Decoder{nil}
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
