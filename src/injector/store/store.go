package store

import (
	"github.com/go-kit/kit/log"
	"github.com/inloco/kafka-elasticsearch-injector/src/elasticsearch"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
)

type Store interface {
	Insert(records []*models.Record) error
	ReadinessCheck() bool
}

type basicStore struct {
	db    elasticsearch.RecordDatabase
	codec elasticsearch.Codec
}

func (s basicStore) Insert(records []*models.Record) error {
	elasticRecords, err := s.codec.EncodeElasticRecords(records)
	if err != nil {
		return err
	}
	return s.db.Insert(elasticRecords)
}

func (s basicStore) ReadinessCheck() bool {
	return s.db.ReadinessCheck()
}

func NewStore(logger log.Logger) Store {
	config := elasticsearch.NewConfig()
	return basicStore{
		db:    elasticsearch.NewDatabase(logger, config),
		codec: elasticsearch.NewCodec(logger, config),
	}
}
