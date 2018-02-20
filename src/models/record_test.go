package models

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var existentFieldName = "existentField"
var existentFieldValue = "dummy-value"
var inexistentFieldName = "inexistentField"

func TestRecord_GetValueForField_ErrorForInexistentField(t *testing.T) {
	record := createDummyRecord(existentFieldName, existentFieldValue)

	_, err := record.GetValueForField(inexistentFieldName)
	assert.Error(t, err)
}

func TestRecord_GetValueForField_FieldValueForExistentField(t *testing.T) {
	record := createDummyRecord(existentFieldName, existentFieldValue)

	value, err := record.GetValueForField(existentFieldName)
	if assert.NoError(t, err) {
		assert.Equal(t, value, existentFieldValue)
	}
}

func TestRecord_RemoveBlacklistedFields_NoOpForInexistentField(t *testing.T) {
	record := createDummyRecord(existentFieldName, existentFieldValue)

	record.RemoveBlacklistedFields([]string{inexistentFieldName})
	assert.Contains(t, record.Json, existentFieldName)
}

func TestRecord_RemoveBlacklistedFields_DeletesExistentField(t *testing.T) {
	record := createDummyRecord(existentFieldName, existentFieldValue)

	record.RemoveBlacklistedFields([]string{existentFieldName})
	assert.NotContains(t, record.Json, existentFieldName)
	assert.Empty(t, record.Json)
}

func createDummyRecord(fieldName string, fieldValue string) *Record {
	return &Record{
		Topic:     "dummy-topic",
		Partition: rand.Int31(),
		Offset:    rand.Int63(),
		Timestamp: time.Now(),
		Json:      map[string]interface{}{existentFieldName: existentFieldValue},
	}
}
