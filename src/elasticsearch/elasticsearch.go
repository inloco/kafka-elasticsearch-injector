package elasticsearch

import (
	"context"

	"fmt"

	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/kafka"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/olivere/elastic"
)

const (
	parentAggregation = "parent_agg"
	subAggregation    = "sub_agg"
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

type visitDatabase struct {
	logger log.Logger
	config Config
}

func (d visitDatabase) GetClient() *elastic.Client {
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

func (d visitDatabase) CloseClient() {
	if esClient != nil {
		esClient.Stop()
	}
}

func (d visitDatabase) Insert(records []*kafka.Record) error {
	bulkRequest := d.GetClient().Bulk()
	for _, record := range records {
		index := fmt.Sprintf("%s-%s", d.config.index, record.FormatTimestamp())
		bulkRequest.Add(elastic.NewBulkIndexRequest().Index(index).
			Type(d.config.index).
			Doc(record.Json))
	}
	timeout := d.config.bulkTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := bulkRequest.Do(ctx)

	return err
}

func (d visitDatabase) ReadinessCheck() bool {
	info, _, err := d.GetClient().Ping(d.config.host).Do(context.Background())
	if err != nil {
		level.Error(d.logger).Log("err", err, "message", "error pinging elasticsearch")
		return false
	}
	level.Info(d.logger).Log("message", fmt.Sprintf("connected to es version %s", info.Version.Number))
	return true
}

func NewDatabase(logger log.Logger, config Config) visitDatabase {
	return visitDatabase{logger: logger, config: config}
}
