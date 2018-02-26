package elasticsearch

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	Host               string
	Index              string
	IndexColumn        string
	BlacklistedColumns []string
	BulkTimeout        time.Duration
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
		Host:               os.Getenv("ELASTICSEARCH_HOST"),
		Index:              os.Getenv("ES_INDEX"),
		IndexColumn:        os.Getenv("ES_INDEX_COLUMN"),
		BlacklistedColumns: strings.Split(os.Getenv("ES_BLACKLISTED_COLUMNS"), ","),
		BulkTimeout:        timeout,
	}
}
