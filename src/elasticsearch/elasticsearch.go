package elasticsearch

import (
	"context"

	"fmt"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/kafka"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/olivere/elastic"
)

var esClient *elastic.Client

type basicDatabase interface {
	GetClient() *elastic.Client
	CloseClient()
}

type RecordDatabase interface {
	basicDatabase
	Insert(records []*kafka.Record) error
	ReadinessCheck() bool
}

type recordDatabase struct {
	logger log.Logger
	config Config
}

func (d recordDatabase) GetClient() *elastic.Client {
	if esClient == nil {
		client, err := elastic.NewClient(elastic.SetURL(d.config.host))
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
	}
}

func (d recordDatabase) Insert(records []*kafka.Record) error {
	bulkRequest := d.GetClient().Bulk()
	for _, record := range records {
		index := fmt.Sprintf("%s-%s", d.config.index, record.FormatTimestamp())
		bulkRequest.Add(elastic.NewBulkIndexRequest().Index(index).
			Type(d.config.index).
			Id(record.GetId()).
			Doc(record.Json))
	}
	timeout := d.config.bulkTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := bulkRequest.Do(ctx)
	if res.Errors {

	}

	return err
}

func (d recordDatabase) ReadinessCheck() bool {
	info, _, err := d.GetClient().Ping(d.config.host).Do(context.Background())
	if err != nil {
		level.Error(d.logger).Log("err", err, "message", "error pinging elasticsearch")
		return false
	}
	level.Info(d.logger).Log("message", fmt.Sprintf("connected to es version %s", info.Version.Number))
	return true
}

func NewDatabase(logger log.Logger, config Config) RecordDatabase {
	return recordDatabase{logger: logger, config: config}
}
