package elasticsearch

import (
	"testing"

	"os"

	"context"

	"fmt"
	"time"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka/fixtures"
	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/logger_builder"
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/assert"
)

var logger = logger_builder.NewLogger("elasticsearch-test")
var config = Config{
	host:        "http://localhost:9200",
	index:       "my-topic",
	bulkTimeout: 10 * time.Second,
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
	templateExists, err := db.GetClient().IndexTemplateExists(config.index).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !templateExists {
		_, err := db.GetClient().IndexPutTemplate(config.index).BodyString(template).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}
	retCode := m.Run()
	db.GetClient().DeleteIndex().Index([]string{"_all"}).Do(context.Background())
	db.CloseClient()
	os.Exit(retCode)
}

func TestDatabase_ReadinessCheck(t *testing.T) {
	ready := db.ReadinessCheck()
	assert.Equal(t, true, ready)
}

func TestDatabase_Insert(t *testing.T) {
	now := time.Now()
	record := fixtures.NewRecord(now)
	index := fmt.Sprintf("%s-%s", config.index, record.FormatTimestamp())
	err := db.Insert([]*kafka.Record{record})
	db.GetClient().Refresh("_all").Do(context.Background())
	if assert.NoError(t, err) {
		count, err := db.GetClient().Count(index).Do(context.Background())
		if assert.NoError(t, err) {
			assert.Equal(t, int64(1), count)
		}
	}
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}
