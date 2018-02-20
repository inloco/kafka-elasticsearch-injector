package elasticsearch

import (
	"context"
	"strconv"

	"fmt"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/models"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
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
	Insert(records []*models.Record) error
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

func getValueForColumn(recordMap map[string]interface{}, indexColumn string) (string, error) {
	if value, ok := recordMap[indexColumn]; ok {
		switch castedValue := value.(type) {
		case string:
			return castedValue, nil
		case int32:
			return strconv.FormatInt(int64(castedValue), 10), nil
		default:
			return "", fmt.Errorf("Value from colum %s is not parseable to string", indexColumn)
		}
	}
	return "", fmt.Errorf("could not get value from column %s", indexColumn)
}

func removeBlacklistedColumns(recordMap *map[string]interface{}, blacklistedColumns []string) {
	for _, blacklistedColumn := range blacklistedColumns {
		delete(*recordMap, blacklistedColumn)
	}
}

func (d recordDatabase) Insert(records []*models.Record) error {
	bulkRequest := d.GetClient().Bulk()
	for _, record := range records {
		indexName := d.config.Index
		if indexName == "" {
			indexName = record.Topic
		}
		indexColumn := d.config.IndexColumn
		indexColumnValue := record.FormatTimestamp()
		if indexColumn != "" {
			newIndexColumnValue, err := getValueForColumn(record.Json, indexColumn)
			if err != nil {
				level.Error(d.logger).Log("err", err, "message", "Could not get column value from record.")
				return err
			}
			indexColumnValue = newIndexColumnValue
		}
		removeBlacklistedColumns(&record.Json, d.config.BlacklistedColumns)
		index := fmt.Sprintf("%s-%s", indexName, indexColumnValue)
		bulkRequest.Add(elastic.NewBulkIndexRequest().Index(index).
			Type(record.Topic).
			Id(record.GetId()).
			Doc(record.Json))
	}
	timeout := d.config.BulkTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := bulkRequest.Do(ctx)
	if err == nil {
		if res.Errors {
			for _, f := range res.Failed() {
				return errors.New(fmt.Sprintf("%s", f.Error))
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

func NewDatabase(logger log.Logger, config Config) RecordDatabase {
	return recordDatabase{logger: logger, config: config}
}
