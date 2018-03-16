package injector

import (
	"github.com/inloco/kafka-elasticsearch-injector/src/injector/store"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
	"github.com/go-kit/kit/log"
)

type Service interface {
	Insert(records []*models.Record) error
	ReadinessCheck() bool
}

type basicService struct {
	store store.Store
}

func (s basicService) Insert(records []*models.Record) error {
	return s.store.Insert(records)
}

func (s basicService) ReadinessCheck() bool {
	return s.store.ReadinessCheck()
}

func NewService(logger log.Logger) Service {
	return basicService{
		store.NewStore(logger),
	}
}
