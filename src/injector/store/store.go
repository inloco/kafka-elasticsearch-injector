package store

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/inloco/kafka-elasticsearch-injector/src/elasticsearch"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
)

type Store interface {
	Insert(records []*models.Record) error
	ReadinessCheck() bool
}

type basicStore struct {
	db      elasticsearch.RecordDatabase
	codec   elasticsearch.Codec
	backoff time.Duration
}

func (s basicStore) Insert(records []*models.Record) error {
	elasticRecords, err := s.codec.EncodeElasticRecords(records)
	if err != nil {
		return err
	}
	for {
		res, err := s.db.Insert(elasticRecords)
		if err != nil {
			return err
		}
		if len(res.Retry) == 0 {
			break
		}
		//some records failed to index, backoff(if overloaded) then retry
		if res.Backoff {
			time.Sleep(s.backoff)
		}
		elasticRecords = res.Retry
	}
	return nil
}

func (s basicStore) ReadinessCheck() bool {
	return s.db.ReadinessCheck()
}

func NewStore(logger log.Logger) Store {
	config := elasticsearch.NewConfig()
	return basicStore{
		db:      elasticsearch.NewDatabase(logger, config),
		codec:   elasticsearch.NewCodec(logger, config),
		backoff: config.Backoff,
	}
}
