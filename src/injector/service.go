package injector

import (
	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/injector/store"
	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/kafka"
	"github.com/go-kit/kit/log"
)

type Service interface {
	Insert(records []*kafka.Record) error
	ReadinessCheck() bool
}

type basicService struct {
	store store.Store
}

func (s basicService) Insert(records []*kafka.Record) error {
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
