package injector

import (
	"time"

	"github.com/inloco/kafka-elasticsearch-injector/src/metrics"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
)

type instrumentingMiddleware struct {
	metricsPublisher metrics.MetricsPublisher
	next             Service
}

func (s instrumentingMiddleware) Insert(records []*models.Record) error {
	begin := time.Now()
	err := s.next.Insert(records)
	s.metricsPublisher.RecordEndpointLatency(time.Since(begin).Seconds())
	return err
}

func (s instrumentingMiddleware) ReadinessCheck() bool {
	return s.next.ReadinessCheck()
}
