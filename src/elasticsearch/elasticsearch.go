package elasticsearch

import (
	"context"

	"fmt"

	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
	"github.com/olivere/elastic"
)

var esClient *elastic.Client

type basicDatabase interface {
	GetClient() *elastic.Client
	CloseClient()
}

type RecordDatabase interface {
	basicDatabase
	Insert(records []*models.ElasticRecord) (*InsertResponse, error)
	ReadinessCheck() bool
}

type recordDatabase struct {
	logger log.Logger
	config Config
}

func (d recordDatabase) GetClient() *elastic.Client {
	if esClient == nil {
		client, err := elastic.NewClient(elastic.SetURL(d.config.Host))
		if err != nil {
			level.Error(d.logger).Log("err", err, "message", "could not init elasticsearch client")
			panic(err)
		}
		esClient = client
	}
	return esClient
}

func (d recordDatabase) CloseClient() {
	if esClient != nil {
		esClient.Stop()
		esClient = nil
	}
}

type InsertResponse struct {
	AlreadyExists []string
	Retry         []*models.ElasticRecord
}

func (d recordDatabase) Insert(records []*models.ElasticRecord) (*InsertResponse, error) {
	bulkRequest, err := d.buildBulkRequest(records)
	if err != nil {
		return nil, err
	}
	timeout := d.config.BulkTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := bulkRequest.Do(ctx)

	if err != nil {
		return nil, err
	}
	if res.Errors {
		created := res.Created()
		var alreadyExistsIds []string
		for _, c := range created {
			if c.Status == http.StatusConflict {
				alreadyExistsIds = append(alreadyExistsIds, c.Id)
				level.Warn(d.logger).Log("message", "document already exists", "id", c.Id)
			}
		}
		failed := res.Failed()
		var retry []*models.ElasticRecord
		if len(failed) > 0 {
			recordMap := make(map[string]*models.ElasticRecord)
			for _, rec := range records {
				recordMap[rec.ID] = rec
			}
			for _, f := range failed {
				if f.Status == http.StatusTooManyRequests {
					//es is overloaded, backoff
					level.Warn(d.logger).Log("message", "insert failed: elasticsearch is overloaded", "failed_id", f.Id)
					retry = append(retry, recordMap[f.Id])
				}
			}
		}
		return &InsertResponse{alreadyExistsIds, retry}, nil
	}

	return &InsertResponse{[]string{}, []*models.ElasticRecord{}}, nil
}

func (d recordDatabase) ReadinessCheck() bool {
	info, _, err := d.GetClient().Ping(d.config.Host).Do(context.Background())
	if err != nil {
		level.Error(d.logger).Log("err", err, "message", "error pinging elasticsearch")
		return false
	}
	level.Info(d.logger).Log("message", fmt.Sprintf("connected to es version %s", info.Version.Number))
	return true
}

func (d recordDatabase) buildBulkRequest(records []*models.ElasticRecord) (*elastic.BulkService, error) {
	bulkRequest := d.GetClient().Bulk()
	for _, record := range records {
		bulkRequest.Add(elastic.NewBulkIndexRequest().OpType("create").
			Index(record.Index).
			Type(record.Type).
			Id(record.ID).
			Doc(record.Json))
	}
	return bulkRequest, nil
}

func NewDatabase(logger log.Logger, config Config) RecordDatabase {
	return recordDatabase{logger: logger, config: config}
}
