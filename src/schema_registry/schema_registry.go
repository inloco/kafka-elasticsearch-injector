package schema_registry

import (
	"sync"

	"github.com/datamountaineer/schema-registry"
)

const INVALID_SCHEMA = "Invalid Schema"

type SchemaRegistry struct {
	Client  schemaregistry.Client
	schemas *sync.Map
}

func (sr *SchemaRegistry) GetSchema(id int32) (string, error) {

	if schema, exists := sr.schemas.Load(id); exists {
		if schemaString, ok := schema.(string); ok {
			return schemaString, nil
		}
	}

	schema, err := sr.Client.GetSchemaById(int(id))
	sr.schemas.Store(id, schema)
	return schema, err
}

func NewSchemaRegistry(url string) (*SchemaRegistry, error) {
	client, err := schemaregistry.NewClient(url)
	if err != nil {
		return nil, err
	}
	return &SchemaRegistry{
		Client:  client,
		schemas: &sync.Map{},
	}, nil
}

type Schema struct {
	Type    string
	Version string
	Subject string
}
