package elasticsearch

import (
	"os"
	"time"
)

type Config struct {
	Host        string
	Index       string
	BulkTimeout time.Duration
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
		Host:        os.Getenv("ELASTICSEARCH_HOST"),
		Index:       os.Getenv("ES_INDEX"),
		BulkTimeout: timeout,
	}
}
