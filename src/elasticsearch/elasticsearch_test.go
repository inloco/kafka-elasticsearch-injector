package elasticsearch

import (
	"testing"

	"os"

	"context"

	"time"

	"encoding/json"

	"strconv"

	"github.com/inloco/kafka-elasticsearch-injector/src/kafka/fixtures"
	"github.com/inloco/kafka-elasticsearch-injector/src/logger_builder"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
	"github.com/olivere/elastic/v7"
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
var db = NewDatabase(logger, config)
var template = `
{
	"template": "my-topic-*",
	"settings": {},
	"mappings": {
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
	},
	"aliases": {}
}
`

func TestMain(m *testing.M) {
	setupDB(db)
	retCode := m.Run()
	tearDownDB(db)
	os.Exit(retCode)
}

func TestRecordDatabase_ReadinessCheck(t *testing.T) {
	ready := db.ReadinessCheck()
	assert.Equal(t, true, ready)
}

func TestRecordDatabase_Insert(t *testing.T) {
	record, id := fixtures.NewElasticRecord()
	_, err := db.Insert([]*models.ElasticRecord{record})
	db.GetClient().Refresh("_all").Do(context.Background())
	var recordFromES fixtures.FixtureRecord
	if assert.NoError(t, err) {
		count, err := db.GetClient().Count(record.Index).Do(context.Background())
		if assert.NoError(t, err) {
			assert.Equal(t, int64(1), count)
		}
		res, err := db.GetClient().Get().Index(record.Index).Type(record.Type).Id(record.ID).Do(context.Background())
		if assert.NoError(t, err) {
			_ = json.Unmarshal(res.Source, &recordFromES)
		}
		assert.Equal(t, recordFromES.Id, id)
	}
	db.GetClient().DeleteByQuery(record.Index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestRecordDatabase_Insert_RepeatedId(t *testing.T) {
	record, id := fixtures.NewElasticRecord()
	_, err := db.Insert([]*models.ElasticRecord{record})
	db.GetClient().Refresh("_all").Do(context.Background())
	res, err := db.Insert([]*models.ElasticRecord{record})
	assert.Len(t, res.AlreadyExists, 1)
	assert.Contains(t, res.AlreadyExists, strconv.Itoa(int(id)))
	var recordFromES fixtures.FixtureRecord
	if assert.NoError(t, err) {
		count, err := db.GetClient().Count(record.Index).Do(context.Background())
		if assert.NoError(t, err) {
			assert.Equal(t, int64(1), count)
		}
		res, err := db.GetClient().Get().Index(record.Index).Type(record.Type).Id(record.ID).Do(context.Background())
		if assert.NoError(t, err) {
			_ = json.Unmarshal(res.Source, &recordFromES)
		}
		assert.Equal(t, recordFromES.Id, id)
	}
	db.GetClient().DeleteByQuery(record.Index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestRecordDatabase_Insert_Multiple(t *testing.T) {
	record, id := fixtures.NewElasticRecord()
	_, err := db.Insert([]*models.ElasticRecord{record, record})
	db.GetClient().Refresh("_all").Do(context.Background())
	var recordFromES fixtures.FixtureRecord
	if assert.NoError(t, err) {
		count, err := db.GetClient().Count(record.Index).Do(context.Background())
		if assert.NoError(t, err) {
			assert.Equal(t, int64(1), count)
		}
		res, err := db.GetClient().Get().Index(record.Index).Type(record.Type).Id(record.ID).Do(context.Background())
		if assert.NoError(t, err) {
			_ = json.Unmarshal(res.Source, &recordFromES)
		}
		assert.Equal(t, recordFromES.Id, id)
	}
	db.GetClient().DeleteByQuery(record.Index).Query(elastic.MatchAllQuery{}).Do(context.Background())
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
