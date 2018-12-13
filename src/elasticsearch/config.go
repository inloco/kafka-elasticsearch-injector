package elasticsearch

import (
	"os"
	"strings"
	"time"
)

type TimeIndexSuffix int

const (
	TimeSuffixDay  TimeIndexSuffix = 0
	TimeSuffixHour TimeIndexSuffix = 1
)

type Config struct {
	Host               string
	Index              string
	IndexColumn        string
	DocIDColumn        string
	BlacklistedColumns []string
	BulkTimeout        time.Duration
	Backoff            time.Duration
	TimeSuffix         TimeIndexSuffix
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
	backoffStr, exists := os.LookupEnv("ES_BULK_BACKOFF")
	backoff := 1 * time.Second
	if exists {
		d, err := time.ParseDuration(backoffStr)
		if err == nil {
			backoff = d
		}
	}
	timeSuffix := TimeSuffixDay
	if suffix := os.Getenv("ES_TIME_SUFFIX"); suffix != "" {
		switch suffix {
		case "hour":
			timeSuffix = TimeSuffixHour
		}
	}
	return Config{
		Host:               os.Getenv("ELASTICSEARCH_HOST"),
		Index:              os.Getenv("ES_INDEX"),
		IndexColumn:        os.Getenv("ES_INDEX_COLUMN"),
		DocIDColumn:        os.Getenv("ES_DOC_ID_COLUMN"),
		BlacklistedColumns: strings.Split(os.Getenv("ES_BLACKLISTED_COLUMNS"), ","),
		BulkTimeout:        timeout,
		Backoff:            backoff,
		TimeSuffix:         timeSuffix,
	}
}
