package schema_registry

import (
	"github.com/datamountaineer/schema-registry"
)

type SchemaRegistry struct {
	Client  schemaregistry.Client
	schemas map[int32]string
}

func (sr *SchemaRegistry) GetSchema(id int32) (string, error) {
	schema, exists := sr.schemas[id]
	if exists {
		return schema, nil
	}
	schema, err := sr.Client.GetSchemaById(int(id))
	sr.schemas[id] = schema
	return schema, err
}

func NewSchemaRegistry(url string) (*SchemaRegistry, error) {
	client, err := schemaregistry.NewClient(url)
	if err != nil {
		return nil, err
	}
	return &SchemaRegistry{
		Client:  client,
		schemas: make(map[int32]string),
	}, nil
}

type Schema struct {
	Type    string
	Version string
	Subject string
}
