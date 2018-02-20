package models

import (
	"strconv"
	"time"

	"fmt"
)

type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Json      map[string]interface{}
}

func (r *Record) FormatTimestamp() string {
	return r.Timestamp.Format("2006-01-02")
}

func (r *Record) GetId() string {
	return fmt.Sprintf("%d:%d", r.Partition, r.Offset)
}

func (r *Record) GetValueForField(field string) (string, error) {
	if value, ok := r.Json[field]; ok {
		switch castedValue := value.(type) {
		case string:
			return castedValue, nil
		case int32:
			return strconv.FormatInt(int64(castedValue), 10), nil
		default:
			return "", fmt.Errorf("Value from colum %s is not parseable to string", field)
		}
	}
	return "", fmt.Errorf("could not get value from column %s", field)
}

func (r *Record) RemoveBlacklistedFields(blacklistedFields []string) {
	for _, blacklistedField := range blacklistedFields {
		delete(r.Json, blacklistedField)
	}
}
