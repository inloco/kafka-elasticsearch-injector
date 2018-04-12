package metrics

import (
	"strconv"

	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/inloco/kafka-elasticsearch-injector/src/logger_builder"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	logger                   log.Logger
	partitionDelay           *kitprometheus.Gauge
	recordsConsumed          *kitprometheus.Counter
	endpointLatencyHistogram *kitprometheus.Summary
	bufferFullGauge          *kitprometheus.Gauge
	lock                     sync.RWMutex
	topicPartitionToOffset   map[string]map[int32]int64
}

func (m *metrics) IncrementRecordsConsumed(count int) {
	m.recordsConsumed.Add(float64(count))
}

func (m *metrics) RecordEndpointLatency(latency float64) {
	m.endpointLatencyHistogram.Observe(latency)
}

func (m *metrics) UpdateOffset(topic string, partition int32, offset int64) {
	m.lock.Lock()
	currentOffset, exists := m.topicPartitionToOffset[topic][partition]
	if !exists || offset > currentOffset {
		_, exists := m.topicPartitionToOffset[topic]
		if !exists {
			m.topicPartitionToOffset[topic] = make(map[int32]int64)
		}
		m.topicPartitionToOffset[topic][partition] = offset
	}
	m.lock.Unlock()
}

func (m *metrics) PublishOffsetMetrics(highWaterMarks map[string]map[int32]int64) {
	for topic, partitions := range highWaterMarks {
		for partition, maxOffset := range partitions {
			m.lock.RLock()
			offset, ok := m.topicPartitionToOffset[topic][partition]
			m.lock.RUnlock()
			if ok {
				delay := maxOffset - offset
				level.Info(m.logger).Log("message", "updating partition offset metric",
					"partition", partition, "maxOffset", maxOffset, "current", offset, "delay", delay)
				m.partitionDelay.
					With("partition", strconv.Itoa(int(partition)), "topic", topic).
					Set(float64(delay))
			}
		}
	}
}

func (m *metrics) BufferFull(full bool) {
	val := 0.0
	if full {
		val = 1.0
	}
	m.bufferFullGauge.Set(val)
}

type MetricsPublisher interface {
	PublishOffsetMetrics(highWaterMarks map[string]map[int32]int64)
	UpdateOffset(topic string, partition int32, delay int64)
	IncrementRecordsConsumed(count int)
	RecordEndpointLatency(latency float64)
	BufferFull(full bool)
}

func NewMetricsPublisher() MetricsPublisher {
	logger := logger_builder.NewLogger("metrics_updater")
	recordsConsumed := kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "kafka_consumer_records_consumed_successfully",
		Help: "Number of records consumed successfully",
	}, []string{})
	partitionDelay := kitprometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
		Name: "kafka_consumer_partition_delay",
		Help: "Kafka consumer partition delay",
	}, []string{"partition", "topic"})
	endpointLatencySummary := kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
		Name: "kafka_consumer_endpoint_latency_histogram_seconds",
		Help: "Kafka consumer endpoint latency histogram in seconds",
	}, []string{})
	bufferFullGauge := kitprometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
		Name: "kafka_consumer_buffer_full",
		Help: "Kafka consumer boolean indicating if app buffer is full",
	}, []string{})
	return &metrics{
		logger:                   logger,
		partitionDelay:           partitionDelay,
		recordsConsumed:          recordsConsumed,
		endpointLatencyHistogram: endpointLatencySummary,
		bufferFullGauge:          bufferFullGauge,
		lock:                     sync.RWMutex{},
		topicPartitionToOffset: make(map[string]map[int32]int64),
	}
}
