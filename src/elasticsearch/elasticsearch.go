package elasticsearch

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
	"github.com/olivere/elastic/v7"
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
	Backoff       bool
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
	if err == elastic.ErrNoClient || errors.Cause(err) == elastic.ErrNoClient {
		_ = level.Warn(d.logger).Log("message", "no elasticsearch node available", "err", err)
		return &InsertResponse{AlreadyExists: nil, Retry: records, Backoff: true}, nil
	}
	if err != nil {
		return nil, err
	}
	if res.Errors {
		created := res.Created()
		var alreadyExistsIds []string
		for _, c := range created {
			if c.Status == http.StatusConflict {
				alreadyExistsIds = append(alreadyExistsIds, c.Id)
			}
		}
		if len(alreadyExistsIds) > 0 {
			level.Warn(d.logger).Log("message", "document already exists", "doc_count", len(alreadyExistsIds))
		}
		failed := res.Failed()
		var retry []*models.ElasticRecord
		overloaded := false
		if len(failed) > 0 {
			recordMap := make(map[string]*models.ElasticRecord)
			for _, rec := range records {
				recordMap[rec.ID] = rec
			}
			for _, f := range failed {
				if f.Status == http.StatusConflict {
					continue
				}
				retry = append(retry, recordMap[f.Id])
				if f.Status == http.StatusTooManyRequests {
					//es is overloaded, backoff
					overloaded = true
				}
			}
			if overloaded {
				level.Warn(d.logger).Log("message", "insert failed: elasticsearch is overloaded", "retry_count", len(retry))
			}
		}
		return &InsertResponse{alreadyExistsIds, retry, overloaded}, nil
	}

	return &InsertResponse{[]string{}, []*models.ElasticRecord{}, false}, nil
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
