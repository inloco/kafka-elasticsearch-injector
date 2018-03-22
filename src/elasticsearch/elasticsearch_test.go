package elasticsearch

import (
	"strconv"
	"testing"

	"os"

	"context"

	"fmt"
	"time"

	"encoding/json"

	"github.com/inloco/kafka-elasticsearch-injector/src/kafka/fixtures"
	"github.com/inloco/kafka-elasticsearch-injector/src/logger_builder"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/assert"
)

var logger = logger_builder.NewLogger("elasticsearch-test")
var config = Config{
	Host:               "http://localhost:9200",
	Index:              "my-topic",
	IndexColumn:        "",
	BlacklistedColumns: []string{},
	BulkTimeout:        10 * time.Second,
}
var configIndexColumnBlacklist = Config{
	Host:               "http://localhost:9200",
	Index:              "my-topic",
	IndexColumn:        "id",
	BlacklistedColumns: []string{"id"},
	BulkTimeout:        10 * time.Second,
}
var configDocIDColumn = Config{
	Host:        "http://localhost:9200",
	Index:       "my-topic",
	BulkTimeout: 10 * time.Second,
	DocIDColumn: "id",
}
var db = NewDatabase(logger, config)
var dbIndexColumnBlacklist = NewDatabase(logger, configIndexColumnBlacklist)
var dbDocIDColumn = NewDatabase(logger, configDocIDColumn)
var template = `
{
	"template": "my-topic-*",
	"settings": {},
	"mappings": {
	  "my-topic": {
		"_source": {
		  "enabled": "true"
		},
		"dynamic_templates": [
		  {
			"strings": {
			  "mapping": {
				"index": "not_analyzed",
				"type": "string"
			  },
			  "match_mapping_type": "string"
			}
		  }
		],
		"properties": {
		  "id": {
		  	"type": "keyword"
		  }
		}
	  }
	},
	"aliases": {}
}
`

func TestMain(m *testing.M) {
	setupDB(db)
	setupDB(dbIndexColumnBlacklist)
	setupDB(dbDocIDColumn)
	retCode := m.Run()
	tearDownDB(db)
	tearDownDB(dbIndexColumnBlacklist)
	tearDownDB(dbDocIDColumn)
	os.Exit(retCode)
}

func TestRecordDatabase_ReadinessCheck(t *testing.T) {
	ready := db.ReadinessCheck()
	assert.Equal(t, true, ready)
}

func TestRecordDatabase_Insert(t *testing.T) {
	now := time.Now()
	record, id, _ := fixtures.NewRecord(now)
	index := fmt.Sprintf("%s-%s", config.Index, record.FormatTimestamp())
	err := db.Insert([]*models.Record{record})
	db.GetClient().Refresh("_all").Do(context.Background())
	var recordFromES fixtures.FixtureRecord
	if assert.NoError(t, err) {
		count, err := db.GetClient().Count(index).Do(context.Background())
		if assert.NoError(t, err) {
			assert.Equal(t, int64(1), count)
		}
		res, err := db.GetClient().Get().Index(index).Type(record.Topic).Id(record.GetId()).Do(context.Background())
		if assert.NoError(t, err) {
			json.Unmarshal(*res.Source, &recordFromES)
		}
		assert.Equal(t, recordFromES.Id, id)
	}
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestRecordDatabase_Insert_Multiple(t *testing.T) {
	now := time.Now()
	record, id, _ := fixtures.NewRecord(now)
	index := fmt.Sprintf("%s-%s", config.Index, record.FormatTimestamp())
	err := db.Insert([]*models.Record{record, record})
	db.GetClient().Refresh("_all").Do(context.Background())
	var recordFromES fixtures.FixtureRecord
	if assert.NoError(t, err) {
		count, err := db.GetClient().Count(index).Do(context.Background())
		if assert.NoError(t, err) {
			assert.Equal(t, int64(1), count)
		}
		res, err := db.GetClient().Get().Index(index).Type(record.Topic).Id(record.GetId()).Do(context.Background())
		if assert.NoError(t, err) {
			json.Unmarshal(*res.Source, &recordFromES)
		}
		assert.Equal(t, recordFromES.Id, id)
	}
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestRecordDatabase_Insert_IndexColumnBlacklist(t *testing.T) {
	now := time.Now()
	record, id, value := fixtures.NewRecord(now)
	index := fmt.Sprintf("%s-%d", config.Index, id)
	err := dbIndexColumnBlacklist.Insert([]*models.Record{record})
	dbIndexColumnBlacklist.GetClient().Refresh("_all").Do(context.Background())
	var recordFromES fixtures.FixtureRecord
	if assert.NoError(t, err) {
		count, err := dbIndexColumnBlacklist.GetClient().Count(index).Do(context.Background())
		if assert.NoError(t, err) {
			assert.Equal(t, int64(1), count)
		}
		res, err := dbIndexColumnBlacklist.GetClient().Get().Index(index).Type(record.Topic).Id(record.GetId()).Do(context.Background())
		if assert.NoError(t, err) {
			json.Unmarshal(*res.Source, &recordFromES)
		}
		assert.Empty(t, recordFromES.Id)
		assert.Equal(t, value, recordFromES.Value)
	}
	dbIndexColumnBlacklist.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestRecordDatabase_Insert_DocIDColumn(t *testing.T) {
	now := time.Now()
	record, id, _ := fixtures.NewRecord(now)
	index := fmt.Sprintf("%s-%s", config.Index, record.FormatTimestamp())
	err := dbDocIDColumn.Insert([]*models.Record{record})
	dbDocIDColumn.GetClient().Refresh("_all").Do(context.Background())
	var recordFromES fixtures.FixtureRecord
	if assert.NoError(t, err) {
		count, err := dbDocIDColumn.GetClient().Count(index).Do(context.Background())
		if assert.NoError(t, err) {
			assert.Equal(t, int64(1), count)
		}
		res, err := dbDocIDColumn.GetClient().Get().Index(index).Type(record.Topic).Id(strconv.Itoa(int(id))).Do(context.Background())
		if assert.NoError(t, err) {
			json.Unmarshal(*res.Source, &recordFromES)
		}
		assert.Equal(t, recordFromES.Id, id)
	}
	dbDocIDColumn.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func setupDB(d RecordDatabase) {
	templateExists, err := d.GetClient().IndexTemplateExists(config.Index).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !templateExists {
		_, err := d.GetClient().IndexPutTemplate(config.Index).BodyString(template).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}
}

func tearDownDB(d RecordDatabase) {
	d.GetClient().DeleteIndex().Index([]string{"_all"}).Do(context.Background())
	d.CloseClient()
}
