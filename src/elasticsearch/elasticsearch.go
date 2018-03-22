package elasticsearch

import (
	"context"

	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
)

var esClient *elastic.Client

type basicDatabase interface {
	GetClient() *elastic.Client
	CloseClient()
}

type RecordDatabase interface {
	basicDatabase
	Insert(records []*models.ElasticRecord) error
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

func (d recordDatabase) Insert(records []*models.ElasticRecord) error {
	bulkRequest, err := d.buildBulkRequest(records)
	if err != nil {
		return err
	}
	timeout := d.config.BulkTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := bulkRequest.Do(ctx)
	if err == nil {
		if res.Errors {
			for _, f := range res.Failed() {
				return errors.New(fmt.Sprintf("%v", f.Error))
			}
		}
	}

	return err
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
		bulkRequest.Add(elastic.NewBulkIndexRequest().Index(record.Index).
			Type(record.Type).
			Id(record.ID).
			Doc(record.Json))
	}
	return bulkRequest, nil
}

func NewDatabase(logger log.Logger, config Config) RecordDatabase {
	return recordDatabase{logger: logger, config: config}
}
