package elasticsearch

import (
	"context"
	"reflect"
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
		switch value.(type) {
		case string:
			return value.(string), nil
		case int32:
			intVal := value.(int32)
			return strconv.FormatInt(int64(intVal), 10), nil
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
		blacklistedColumns := d.config.BlacklistedColumns
		indexColumnValue := record.FormatTimestamp()
		if indexColumn != "" || len(blacklistedColumns) > 0 {
			recordMap := make(map[string]interface{})
			jsonNativeType := reflect.ValueOf(record.Json)
			if jsonNativeType.Kind() != reflect.Map {
				return fmt.Errorf("could not unmarshall record JSON into map")
			}
			for _, key := range jsonNativeType.MapKeys() {
				if key.Kind() != reflect.String {
					return fmt.Errorf("could not unmarshall record JSON into map keyed by string")
				}
				recordMap[key.String()] = jsonNativeType.MapIndex(key).Interface()
			}
			if indexColumn != "" {
				newIndexColumnValue, err := getValueForColumn(recordMap, indexColumn)
				indexColumnValue = newIndexColumnValue
				if err != nil {
					level.Error(d.logger).Log("err", err, "message", "Could not get column value from record.")
					return err
				}
			}
			if len(blacklistedColumns) > 0 {
				removeBlacklistedColumns(&recordMap, blacklistedColumns)
				record.Json = recordMap
			}
		}
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
