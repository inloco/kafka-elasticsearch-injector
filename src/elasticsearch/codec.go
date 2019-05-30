package elasticsearch

import (
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
)

const typeDoc = "_doc"

type Codec interface {
	EncodeElasticRecords(records []*models.Record) ([]*models.ElasticRecord, error)
}

type basicCodec struct {
	config Config
	logger log.Logger
}

func NewCodec(logger log.Logger, config Config) Codec {
	return basicCodec{logger: logger, config: config}
}

func (c basicCodec) EncodeElasticRecords(records []*models.Record) ([]*models.ElasticRecord, error) {
	elasticRecords := make([]*models.ElasticRecord, len(records))
	for idx, record := range records {
		index, err := c.getDatabaseIndex(record)
		if err != nil {
			return nil, err
		}

		docID, err := c.getDatabaseDocID(record)
		if err != nil {
			return nil, err
		}

		elasticRecords[idx] = &models.ElasticRecord{
			Index: index,
			Type:  typeDoc,
			ID:    docID,
			Json:  record.FilteredFieldsJSON(c.config.BlacklistedColumns),
		}
	}

	return elasticRecords, nil
}

func (c basicCodec) getDatabaseIndex(record *models.Record) (string, error) {
	indexPrefix := c.config.Index
	if indexPrefix == "" {
		indexPrefix = record.Topic
	}

	indexColumn := c.config.IndexColumn
	indexSuffix := record.FormatTimestampDay()
	if c.config.TimeSuffix == TimeSuffixHour {
		indexSuffix = record.FormatTimestampHour()
	}
	if indexColumn != "" {
		newIndexSuffix, err := record.GetValueForField(indexColumn)
		if err != nil {
			level.Error(c.logger).Log("err", err, "message", "Could not get column value from record.")
			return "", err
		}
		indexSuffix = newIndexSuffix
	}

	return fmt.Sprintf("%s-%s", indexPrefix, indexSuffix), nil
}

func (c basicCodec) getDatabaseDocID(record *models.Record) (string, error) {
	docID := record.GetId()

	docIDColumn := c.config.DocIDColumn
	if docIDColumn != "" {
		newDocID, err := record.GetValueForField(docIDColumn)
		if err != nil {
			level.Error(c.logger).Log("err", err, "message", "Could not get doc id value from record.")
			return "", err
		}
		docID = newDocID
	}
	return docID, nil
}
