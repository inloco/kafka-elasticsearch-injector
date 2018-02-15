package schema_registry

import (
	"fmt"
	"sync"

	"github.com/datamountaineer/schema-registry"
)

const INVALID_SCHEMA = "Invalid Schema"

type SchemaRegistry struct {
	Client  schemaregistry.Client
	schemas *sync.Map
}

func (sr *SchemaRegistry) GetSchema(id int32) (string, error) {
	schema, exists := sr.schemas.Load(id)
	if exists {
		schemaString, ok := schema.(string)
		if ok {
			return schemaString, nil
		}
	}
	schema, err := sr.Client.GetSchemaById(int(id))
	schemaString, ok := schema.(string)
	if ok {
		sr.schemas.Store(id, schema)
		return schemaString, err
	}
	return INVALID_SCHEMA, fmt.Errorf("Schema received is not an string")
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
