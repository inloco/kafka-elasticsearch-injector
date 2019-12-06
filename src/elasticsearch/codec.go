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
	logger log.Logger
}

func NewCodec(logger log.Logger) Codec {
	return basicCodec{logger: logger}
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
			Json:  record.FilteredFieldsJSON(nil),
		}
	}

	return elasticRecords, nil
}

func (c basicCodec) getDatabaseIndex(record *models.Record) (string, error) {
	// indexPrefix := "xalala"
	// if indexPrefix == "" {
	// 	indexPrefix = record.Topic
	// }

	// indexColumn := ""
	// indexSuffix := record.FormatTimestampDay()
	// if 0 == TimeSuffixHour {
	// 	indexSuffix = record.FormatTimestampHour()
	// }
	// if indexColumn != "" {
	// 	newIndexSuffix, err := record.GetValueForField(indexColumn)
	// 	if err != nil {
	// 		level.Error(c.logger).Log("err", err, "message", "Could not get column value from record.")
	// 		return "", err
	// 	}
	// 	indexSuffix = newIndexSuffix
	// }

	return fmt.Sprintf("%s-%s", "xalala", "xalala"), nil
}

func (c basicCodec) getDatabaseDocID(record *models.Record) (string, error) {
	docID := record.GetId()

	docIDColumn := ""
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
