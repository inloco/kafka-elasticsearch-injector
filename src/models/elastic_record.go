package models

type ElasticRecord struct {
	Index string
	Type  string
	ID    string
	Json  map[string]interface{}
}
