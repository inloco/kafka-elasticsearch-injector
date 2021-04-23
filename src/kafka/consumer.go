package kafka

import (
	"context"
	"errors"
	"os"

	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	e "github.com/inloco/kafka-elasticsearch-injector/src/errors"
	"github.com/inloco/kafka-elasticsearch-injector/src/metrics"
	"github.com/inloco/kafka-elasticsearch-injector/src/models"
)

type Notification int32

const (
	Ready Notification = iota
	Inserted
)

type kafka struct {
	consumer         Consumer
	consumerCh       chan *sarama.ConsumerMessage
	offsetCh         chan *topicPartitionOffset
	config           *cluster.Config
	brokers          []string
	metricsPublisher metrics.MetricsPublisher
}

type Consumer struct {
	Topics                []string
	Group                 string
	Endpoint              endpoint.Endpoint
	Decoder               DecodeMessageFunc
	Logger                log.Logger
	Concurrency           int
	BatchSize             int
	BatchDeadline         time.Duration
	MetricsUpdateInterval time.Duration
	BufferSize            int
	IncludeKey            bool
}

type topicPartitionOffset struct {
	topic     string
	partition int32
	offset    int64
}

func NewKafka(address string, consumer Consumer, metrics metrics.MetricsPublisher) kafka {
	brokers := []string{address}
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	config.Version = sarama.V0_10_0_0

	return kafka{
		brokers:          brokers,
		config:           config,
		consumer:         consumer,
		metricsPublisher: metrics,
		consumerCh:       make(chan *sarama.ConsumerMessage, consumer.BufferSize),
		offsetCh:         make(chan *topicPartitionOffset),
	}
}

func (k *kafka) Start(signals chan os.Signal, notifications chan<- Notification) {
	topics := k.consumer.Topics
	concurrency := k.consumer.Concurrency
	consumer, err := cluster.NewConsumer(k.brokers, k.consumer.Group, topics, k.config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	buffSize := k.consumer.BatchSize
	batchDeadline := k.consumer.BatchDeadline
	for i := 0; i < concurrency; i++ {
		go k.worker(consumer, buffSize, batchDeadline, notifications)
	}
	go func() {
		for {
			offset := <-k.offsetCh
			k.metricsPublisher.UpdateOffset(offset.topic, offset.partition, offset.offset)
		}
	}()

	go func() {
		for range time.Tick(k.consumer.MetricsUpdateInterval) {
			k.metricsPublisher.PublishOffsetMetrics(consumer.HighWaterMarks())
		}
	}()

	// consume messages, watch errors and notifications
	for {
		select {
		case msg, more := <-consumer.Messages():
			if more {
				if len(k.consumerCh) >= cap(k.consumerCh) {
					level.Warn(k.consumer.Logger).Log(
						"message", "Buffer is full ",
						"channelSize", cap(k.consumerCh),
					)
					k.metricsPublisher.BufferFull(true)
				}
				k.consumerCh <- msg
				k.metricsPublisher.BufferFull(false)
			}
		case err, more := <-consumer.Errors():
			if more {
				level.Error(k.consumer.Logger).Log(
					"message", "Failed to consume message",
					"err", err.Error(),
				)
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				level.Info(k.consumer.Logger).Log(
					"message", "Partitions rebalanced",
					"notification", ntf,
				)
				if ntf.Type == cluster.RebalanceOK {
					notifications <- Ready
				}
			}
		case <-signals:
			return
		}
	}
}

func (k *kafka) decodeMessages(buf []*sarama.ConsumerMessage, bufIdx int) []*models.Record {
	decoded := make([]*models.Record, 0)
	for i := 0; i < bufIdx; i++ {
		req, err := k.consumer.Decoder(nil, buf[i], k.consumer.IncludeKey)
		if err != nil {
			if errors.Is(err, e.ErrNilMessage) {
				continue
			}

			level.Error(k.consumer.Logger).Log(
				"message", "Error decoding message",
				"err", err.Error(),
			)
			continue
		}
		decoded = append(decoded, req)
	}

	return decoded
}

func (k *kafka) flushMessages(buf []*sarama.ConsumerMessage, bufIdx int, consumer *cluster.Consumer, notifications chan<- Notification) {
	records := k.decodeMessages(buf, bufIdx)
	for {
		if res, err := k.consumer.Endpoint(context.Background(), records); err != nil {
			level.Error(k.consumer.Logger).Log("message", "error on endpoint call", "err", err.Error())
			var _ = res // ignore res (for now)
			continue
		}
		break
	}

	notifications <- Inserted
	k.metricsPublisher.IncrementRecordsConsumed(len(records))
	for i := 0; i < bufIdx; i++ {
		k.offsetCh <- &topicPartitionOffset{buf[i].Topic, buf[i].Partition, buf[i].Offset}
		consumer.MarkOffset(buf[i], "") // mark message as processed
	}
}

func (k *kafka) worker(consumer *cluster.Consumer, buffSize int, batchDeadline time.Duration, notifications chan<- Notification) {
	buf := make([]*sarama.ConsumerMessage, buffSize)
	var lastReceivedMsg time.Time
	idx := 0
	for {
		select {
		case kafkaMsg := <-k.consumerCh:
			lastReceivedMsg = time.Now()
			buf[idx] = kafkaMsg
			idx++
			if idx == buffSize {
				k.flushMessages(buf, idx, consumer, notifications)
				idx = 0
			}
		default:
			if idx > 0 && time.Since(lastReceivedMsg) > batchDeadline {
				k.flushMessages(buf, idx, consumer, notifications)
				idx = 0
			}
		}
	}
}
