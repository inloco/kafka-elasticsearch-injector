package kafka

const (
	KafkaConsumer = "consumer"
)

type Config struct {
	Type                  string
	Topics                []string
	ConsumerGroup         string
	Concurrency           string
	BatchSize             string
	MetricsUpdateInterval string
	BufferSize            string
	RecordType            string
}

type RecordType int

const (
	RecordTypeAvro = iota
	RecordTypeJson
)
