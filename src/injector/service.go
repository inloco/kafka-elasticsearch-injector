package injector

import (
	"github.com/go-kit/kit/log"
	"github.com/inloco/kafka-elasticsearch-injector/src/injector/store"
	"github.com/inloco/kafka-elasticsearch-injector/src/metrics"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
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

func NewService(logger log.Logger, metrics metrics.MetricsPublisher) Service {
	return instrumentingMiddleware{
		metricsPublisher: metrics,
		next: basicService{
			store.NewStore(logger, metrics),
		},
	}
}
