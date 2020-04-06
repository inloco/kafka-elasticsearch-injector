package kafka

const (
	ConsumerType = "consumer"
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
	IncludeKey            string
}
