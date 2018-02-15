package models

import (
	"time"

	"fmt"
)

type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Json      interface{}
}

func (r *Record) FormatTimestamp() string {
	return r.Timestamp.Format("2006-01-02")
}

func (r *Record) GetId() string {
	return fmt.Sprintf("%d:%d", r.Partition, r.Offset)
}
