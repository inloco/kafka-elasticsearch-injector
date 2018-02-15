package kafka

import (
	"strconv"

	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

var (
	partitionDelay = kitprometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
		Name: "kafka_consumer_partition_delay",
		Help: "Kafka consumer partition delay",
	}, []string{"partition", "topic"})
)

func updateOffset(topic string, partition int32, delay int64) {
	partitionDelay.
		With("partition", strconv.Itoa(int(partition)), "topic", topic).
		Set(float64(delay))
}
