package elasticsearch

import (
	"os"
	"time"
)

type Config struct {
	host        string
	index       string
	tp          string
	bulkTimeout time.Duration
}

func NewConfig() Config {
	timeoutStr, exists := os.LookupEnv("ES_BULK_TIMEOUT")
	timeout := 1 * time.Second
	if exists {
		d, err := time.ParseDuration(timeoutStr)
		if err == nil {
			timeout = d
		}
	}
	return Config{
		host:        os.Getenv("ELASTICSEARCH_HOST"),
		index:       os.Getenv("ES_INDEX"),
		tp:          os.Getenv("ES_TYPE"),
		bulkTimeout: timeout,
	}
}
