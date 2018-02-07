package elasticsearch

import (
	"testing"

	"os"

	"context"

	"fmt"
	"time"

	"encoding/json"

	"bitbucket.org/ubeedev/engage/src/logger_builder"
	"bitbucket.org/ubeedev/engage/src/visit_stats"
	"bitbucket.org/ubeedev/engage/src/visits"
	"bitbucket.org/ubeedev/visit-datamart-api/src/visits/avro/fixtures"
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/assert"
)

var logger = logger_builder.NewLogger("elasticsearch-test")
var config = Config{
	host:        "http://localhost:9200",
	index:       "visits",
	tp:          "visits",
	bulkTimeout: 10 * time.Second,
}
var db = NewDatabase(logger, config)
var template = `
{
	"index_patterns": [
	  "visits-*"
	],
	"settings": {},
	"mappings": {
	  "visits": {
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
		  "app_id": {
		  	"type": "keyword"
		  },
		  "geoareas": {
			"type": "keyword"
		  },
		  "geohash": {
			"type": "keyword"
		  },
		  "places": {
			"type": "nested",
			"dynamic": false,
			"properties": {
				"id": {
					"type": "keyword"
				},
				"labels": {
					"type": "keyword"
				},
				"store_chain_ids": {
					"type": "keyword"
				},
				"user_context": {
					"type": "keyword"
				},
				"reliability": {
					"type": "keyword"
				}
			}
		  },
		  "timestamp": {
			"format": "epoch_millis",
			"ignore_malformed": true,
			"type": "date"
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

func TestVisitDatabase_ReadinessCheck(t *testing.T) {
	ready := db.ReadinessCheck()
	assert.Equal(t, true, ready)
}

func TestVisitDatabase_Insert(t *testing.T) {
	visit := fixtures.NewVisit()
	tm := time.Unix(0, visit.Timestamp*int64(time.Millisecond)).UTC()
	index := fmt.Sprintf("%s-%s", config.index, tm.Format("2006-01-02"))
	err := db.Insert([]*visits.Visit{&visit})
	db.GetClient().Refresh("visits-*").Do(context.Background())
	var visitJson Visit
	if assert.NoError(t, err) {
		res, err := db.GetClient().Get().Index(index).Type(config.tp).Id(visit.Id).Do(context.Background())
		if assert.NoError(t, err) {
			json.Unmarshal(*res.Source, &visitJson)
		}
	}
	assert.Equal(t, visit.AppId, visitJson.AppId)
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestVisitDatabase_Insert_WithFullVisit(t *testing.T) {
	visit := fixtures.NewVisit()
	tm := time.Unix(0, visit.Timestamp*int64(time.Millisecond)).UTC()
	index := fmt.Sprintf("%s-%s", config.index, tm.Format("2006-01-02"))
	err := db.Insert([]*visits.Visit{&visit})
	db.GetClient().Refresh("visits-*").Do(context.Background())
	var visitJson Visit
	if assert.NoError(t, err) {
		res, err := db.GetClient().Get().Index(index).Type(config.tp).Id(visit.Id).Do(context.Background())
		if assert.NoError(t, err) {
			json.Unmarshal(*res.Source, &visitJson)
		}
	}
	expected := visitToWritable(&visit, config.index).visit
	assert.Equal(t, *expected, visitJson)
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestVisitDatabase_GetVisitStats(t *testing.T) {
	visit := fixtures.NewVisit()
	tm := time.Unix(0, visit.Timestamp*int64(time.Millisecond)).UTC()
	req := visit_stats.VisitStatsRequest{
		AppId: visit.AppId,
		TimeFilter: &visit_stats.ExactTimeFilter{
			FilterType: visit_stats.Day,
			Time:       &tm,
		},
	}
	err := db.Insert([]*visits.Visit{&visit})
	db.GetClient().Refresh("visits-*").Do(context.Background())
	expectedRes := visit_stats.VisitStatsResponse{
		VisitStatsList: []visit_stats.VisitStats{
			{Count: 1},
		},
	}
	if assert.NoError(t, err) {
		res, err := db.GetVisitStats(&req)
		if assert.NoError(t, err) {
			assert.Equal(t, *res, expectedRes)
		}
	}
	index := fmt.Sprintf("%s-%s", config.index, tm.Format("2006-01-02"))
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestVisitDatabase_GetVisitStats_WithNestedFilter(t *testing.T) {
	visit := fixtures.NewVisit()
	tm := time.Unix(0, visit.Timestamp*int64(time.Millisecond)).UTC()
	req := visit_stats.VisitStatsRequest{
		PlaceId: visit.Places[0].Id,
		TimeFilter: &visit_stats.ExactTimeFilter{
			FilterType: visit_stats.Day,
			Time:       &tm,
		},
	}
	err := db.Insert([]*visits.Visit{&visit})
	db.GetClient().Refresh("visits-*").Do(context.Background())
	expectedRes := visit_stats.VisitStatsResponse{
		VisitStatsList: []visit_stats.VisitStats{
			{Count: 1},
		},
	}
	if assert.NoError(t, err) {
		res, err := db.GetVisitStats(&req)
		if assert.NoError(t, err) {
			assert.Equal(t, *res, expectedRes)
		}
	}
	index := fmt.Sprintf("%s-%s", config.index, tm.Format("2006-01-02"))
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestVisitDatabase_GetVisitStatsWithRangeFilter(t *testing.T) {
	visit := fixtures.NewVisit()
	visit2 := fixtures.NewDecodedVisit()
	visit2.Timestamp = visit.Timestamp + int64((24*time.Hour).Seconds()*1000)
	tm := time.Unix(0, visit.Timestamp*int64(time.Millisecond)).UTC()
	tomorrow := tm.AddDate(0, 0, 1)
	req := visit_stats.VisitStatsRequest{
		AppId: visit.AppId,
		TimeFilter: &visit_stats.RangeTimeFilter{
			FilterType: visit_stats.Day,
			Start:      &tm,
			End:        &tomorrow,
		},
	}
	err := db.Insert([]*visits.Visit{&visit, &visit2})
	db.GetClient().Refresh("visits-*").Do(context.Background())
	expectedRes := visit_stats.VisitStatsResponse{
		VisitStatsList: []visit_stats.VisitStats{
			{Count: 2},
		},
	}
	if assert.NoError(t, err) {
		res, err := db.GetVisitStats(&req)
		if assert.NoError(t, err) {
			assert.Equal(t, *res, expectedRes)
		}
	}
	index := fmt.Sprintf("%s-%s", config.index, tm.Format("2006-01-02"))
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestVisitDatabase_GetVisitStatsWithRankBy(t *testing.T) {
	visit := fixtures.NewVisit()
	tm := time.Unix(0, visit.Timestamp*int64(time.Millisecond)).UTC()
	req := visit_stats.VisitStatsRequest{
		AppId:  visit.AppId,
		RankBy: &visit_stats.RankBy{Field: "label", Count: 10},
		TimeFilter: &visit_stats.ExactTimeFilter{
			FilterType: visit_stats.Day,
			Time:       &tm,
		},
	}
	err := db.Insert([]*visits.Visit{&visit})
	db.GetClient().Refresh("visits-*").Do(context.Background())
	var stats []visit_stats.VisitStats
	for _, place := range visit.Places {
		for _, label := range place.Labels {
			stats = append(stats, visit_stats.VisitStats{Count: 1, RankValue: label})
		}
	}
	expectedRes := visit_stats.VisitStatsResponse{VisitStatsList: stats}
	if assert.NoError(t, err) {
		res, err := db.GetVisitStats(&req)
		if assert.NoError(t, err) {
			assert.Equal(t, *res, expectedRes)
		}
	}
	index := fmt.Sprintf("%s-%s", config.index, tm.Format("2006-01-02"))
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestVisitDatabase_GetVisitStatsWithRankBy_AppId(t *testing.T) {
	visit := fixtures.NewVisit()
	tm := time.Unix(0, visit.Timestamp*int64(time.Millisecond)).UTC()
	req := visit_stats.VisitStatsRequest{
		RankBy: &visit_stats.RankBy{Field: "app", Count: 10},
		TimeFilter: &visit_stats.ExactTimeFilter{
			FilterType: visit_stats.Day,
			Time:       &tm,
		},
	}
	err := db.Insert([]*visits.Visit{&visit})
	db.GetClient().Refresh("visits-*").Do(context.Background())
	stats := []visit_stats.VisitStats{{Count: 1, RankValue: visit.AppId}}

	expectedRes := visit_stats.VisitStatsResponse{VisitStatsList: stats}
	if assert.NoError(t, err) {
		res, err := db.GetVisitStats(&req)
		if assert.NoError(t, err) {
			assert.Equal(t, *res, expectedRes)
		}
	}
	index := fmt.Sprintf("%s-%s", config.index, tm.Format("2006-01-02"))
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}

func TestVisitDatabase_GetVisitStats_WithReliabilityFilter(t *testing.T) {
	visit := fixtures.NewVisit()
	tm := time.Unix(0, visit.Timestamp*int64(time.Millisecond)).UTC()
	placeId := "12345"
	req := visit_stats.VisitStatsRequest{
		MinimumReliability: "HIGH",
		PlaceId:            placeId,
		TimeFilter: &visit_stats.ExactTimeFilter{
			FilterType: visit_stats.Day,
			Time:       &tm,
		},
	}
	visit.Places = append(visit.Places, &visits.Place{
		Id:          placeId,
		Labels:      []string{"restaurant"},
		Reliability: 1,
	})
	err := db.Insert([]*visits.Visit{&visit})
	db.GetClient().Refresh("visits-*").Do(context.Background())
	stats := []visit_stats.VisitStats{{Count: 0}}

	expectedRes := visit_stats.VisitStatsResponse{VisitStatsList: stats}
	if assert.NoError(t, err) {
		res, err := db.GetVisitStats(&req)
		if assert.NoError(t, err) {
			assert.Equal(t, *res, expectedRes)
		}
	}
	index := fmt.Sprintf("%s-%s", config.index, tm.Format("2006-01-02"))
	db.GetClient().DeleteByQuery(index).Query(elastic.MatchAllQuery{}).Do(context.Background())
}
