package schema_registry

import (
	"github.com/datamountaineer/schema-registry"
)

const (
	SchemaTypeValue = "value"
)

type SchemaRegistry struct {
	Client  schemaregistry.Client
	schemas map[string]map[int32]string
}

func (sr *SchemaRegistry) GetSchema(subject string, version int32) (string, error) {
	byVersion, existsSubject := sr.schemas[subject]
	if existsSubject {
		if schemaStr, exists := byVersion[version]; exists {
			return schemaStr, nil
		}
	}
	schema, err := sr.Client.GetSchemaBySubject(subject, int(version))
	if !existsSubject {
		sr.schemas[subject] = make(map[int32]string)
	}
	sr.schemas[subject][version] = schema.Schema
	return schema.Schema, err
}

func NewSchemaRegistry(url string) (*SchemaRegistry, error) {
	client, err := schemaregistry.NewClient(url)
	if err != nil {
		return nil, err
	}
	return &SchemaRegistry{
		Client:  client,
		schemas: make(map[string]map[int32]string),
	}, nil
}

type Schema struct {
	Type    string
	Version string
	Subject string
}
