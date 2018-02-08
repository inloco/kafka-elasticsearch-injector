package elasticsearch

import (
	"testing"

	"os"

	"context"

	"fmt"
	"time"

	"encoding/json"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka/fixtures"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/logger_builder"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/models"
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/assert"
)

var logger = logger_builder.NewLogger("elasticsearch-test")
var config = Config{
	Host:        "http://localhost:9200",
	Index:       "my-topic",
	BulkTimeout: 10 * time.Second,
}
var db = NewDatabase(logger, config)
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
	templateExists, err := db.GetClient().IndexTemplateExists(config.Index).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !templateExists {
		_, err := db.GetClient().IndexPutTemplate(config.Index).BodyString(template).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}
	retCode := m.Run()
	db.GetClient().DeleteIndex().Index([]string{"_all"}).Do(context.Background())
	db.CloseClient()
	os.Exit(retCode)
}

func TestRecordDatabase_ReadinessCheck(t *testing.T) {
	ready := db.ReadinessCheck()
	assert.Equal(t, true, ready)
}

func TestRecordDatabase_Insert(t *testing.T) {
	now := time.Now()
	record, id := fixtures.NewRecord(now)
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
	record, id := fixtures.NewRecord(now)
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
