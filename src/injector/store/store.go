package store

import (
	"github.com/inloco/kafka-elasticsearch-injector/src/elasticsearch"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
	"github.com/go-kit/kit/log"
)

type Store interface {
	Insert(records []*models.Record) error
	ReadinessCheck() bool
}

type basicStore struct {
	db elasticsearch.RecordDatabase
}

func (s basicStore) Insert(records []*models.Record) error {
	return s.db.Insert(records)
}

func (s basicStore) ReadinessCheck() bool {
	return s.db.ReadinessCheck()
}

func NewStore(logger log.Logger) Store {
	return basicStore{elasticsearch.NewDatabase(logger, elasticsearch.NewConfig())}
}
