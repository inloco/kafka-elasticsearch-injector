package elasticsearch

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/inloco/kafka-elasticsearch-injector/src/kafka/fixtures"
	"github.com/inloco/kafka-elasticsearch-injector/src/logger_builder"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
)

var codecLogger = logger_builder.NewLogger("elasticsearch-test")

func TestCodec_EncodeElasticRecords(t *testing.T) {
	codec := &basicCodec{
		config: Config{},
		logger: codecLogger,
	}
	record, id, value := fixtures.NewRecord(time.Now())

	elasticRecords, err := codec.EncodeElasticRecords([]*models.Record{record})
	if assert.NoError(t, err) && assert.Len(t, elasticRecords, 1) {
		elasticRecord := elasticRecords[0]
		assert.Equal(t, fmt.Sprintf("%s-%s", record.Topic, record.FormatTimestampDay()), elasticRecord.Index)
		assert.Equal(t, "_doc", elasticRecord.Type)
		assert.Equal(t, fmt.Sprintf("%d:%d", record.Partition, record.Offset), elasticRecord.ID)
		assert.Equal(t, id, elasticRecord.Json["id"])
		assert.Equal(t, value, elasticRecord.Json["value"])
	}
}

func TestCodec_EncodeElasticRecordsHourSuffix(t *testing.T) {
	codec := &basicCodec{
		config: Config{
			TimeSuffix: TimeSuffixHour,
		},
		logger: codecLogger,
	}
	record, id, value := fixtures.NewRecord(time.Now())

	elasticRecords, err := codec.EncodeElasticRecords([]*models.Record{record})
	if assert.NoError(t, err) && assert.Len(t, elasticRecords, 1) {
		elasticRecord := elasticRecords[0]
		assert.Equal(t, fmt.Sprintf("%s-%s", record.Topic, record.FormatTimestampHour()), elasticRecord.Index)
		assert.Equal(t, "_doc", elasticRecord.Type)
		assert.Equal(t, fmt.Sprintf("%d:%d", record.Partition, record.Offset), elasticRecord.ID)
		assert.Equal(t, id, elasticRecord.Json["id"])
		assert.Equal(t, value, elasticRecord.Json["value"])
	}
}

func TestCodec_EncodeElasticRecords_ColumnsBlacklist(t *testing.T) {
	codec := &basicCodec{
		config: Config{BlacklistedColumns: []string{"value"}},
		logger: codecLogger,
	}
	record, _, _ := fixtures.NewRecord(time.Now())

	elasticRecords, err := codec.EncodeElasticRecords([]*models.Record{record})
	if assert.NoError(t, err) && assert.Len(t, elasticRecords, 1) {
		elasticRecord := elasticRecords[0]
		assert.Contains(t, elasticRecord.Json, "id")
		assert.NotContains(t, elasticRecord.Json, "value")
	}
}

func TestCodec_EncodeElasticRecords_IndexColumn(t *testing.T) {
	indexPrefix := "prefix"

	codec := &basicCodec{
		config: Config{Index: indexPrefix, IndexColumn: "id"},
		logger: codecLogger,
	}
	record, id, _ := fixtures.NewRecord(time.Now())

	elasticRecords, err := codec.EncodeElasticRecords([]*models.Record{record})
	if assert.NoError(t, err) && assert.Len(t, elasticRecords, 1) {
		elasticRecord := elasticRecords[0]
		assert.Equal(t, fmt.Sprintf("%v-%v", indexPrefix, id), elasticRecord.Index)
	}
}

func TestCodec_EncodeElasticRecords_InexistentIndexColumn(t *testing.T) {
	codec := &basicCodec{
		config: Config{IndexColumn: "invalid"},
		logger: codecLogger,
	}
	record, _, _ := fixtures.NewRecord(time.Now())

	_, err := codec.EncodeElasticRecords([]*models.Record{record})
	assert.Error(t, err)
}

func TestCodec_EncodeElasticRecords_DocIDColumn(t *testing.T) {
	codec := &basicCodec{
		config: Config{DocIDColumn: "id"},
		logger: codecLogger,
	}
	record, id, _ := fixtures.NewRecord(time.Now())

	elasticRecords, err := codec.EncodeElasticRecords([]*models.Record{record})
	if assert.NoError(t, err) && assert.Len(t, elasticRecords, 1) {
		elasticRecord := elasticRecords[0]
		assert.Equal(t, strconv.Itoa(int(id)), elasticRecord.ID)
	}
}

func TestCodec_EncodeElasticRecords_InexistentDocIDColumn(t *testing.T) {
	codec := &basicCodec{
		config: Config{DocIDColumn: "invalid"},
		logger: codecLogger,
	}
	record, _, _ := fixtures.NewRecord(time.Now())

	_, err := codec.EncodeElasticRecords([]*models.Record{record})
	assert.Error(t, err)
}
