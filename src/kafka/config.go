package kafka

const (
	KafkaConsumer = "consumer"
)

type Config struct {
	Type          string
	Topic         string
	ConsumerGroup string
	Concurrency   string
	BatchSize     string
}
