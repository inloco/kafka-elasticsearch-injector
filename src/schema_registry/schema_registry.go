package schema_registry

import (
	"strconv"

	"github.com/datamountaineer/schema-registry"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

const (
	SchemaTypeValue = "value"
)

type SchemaRegistry struct {
	Client  schemaregistry.Client
	schemas map[string]map[int32]string
	Value   *Schema
	Key     *Schema
}

func (sr *SchemaRegistry) ValidateSchema(logger log.Logger) error {
	version, err := strconv.Atoi(sr.Value.Version)
	if err != nil {
		return err
	}

	schema, err := sr.Client.GetLatestSchema(sr.Value.Subject)
	if err != nil {
		return err
	}

	level.Debug(logger).Log(
		"message", "Latest schema fetched from registry",
		"type", sr.Value.Type,
		"latestSchema", schema.Schema)

	if version != schema.Version {
		level.Warn(logger).Log(
			"message", "The schema from the schema registry has a different version from the compiled schema",
			"compiled_version", version, "schema_registry_version", schema.Version)
	}

	return nil
}

func (sr *SchemaRegistry) GetSchema(subject string, version int32) (string, error) {
	if byVersion, exists := sr.schemas[subject]; exists {
		if schemaStr, exists := byVersion[version]; exists {
			return schemaStr, nil
		}
	}
	schema, err := sr.Client.GetSchemaBySubject(subject, int(version))
	sr.schemas[subject][version] = schema.Schema
	return schema.Schema, err
}

func NewSchemaRegistry(url string, value *Schema) (*SchemaRegistry, error) {
	client, err := schemaregistry.NewClient(url)
	if err != nil {
		return nil, err
	}
	return &SchemaRegistry{
		Client:  client,
		schemas: make(map[string]map[int32]string),
		Value:   value,
	}, nil
}

type Schema struct {
	Type    string
	Version string
	Subject string
}

func (s *Schema) GetSchemaID(logger log.Logger, client schemaregistry.Client) (int, error) {

	version, err := strconv.Atoi(s.Version)
	if err != nil {
		return 0, err
	}

	latestSchema, err := client.GetLatestSchema(s.Subject)
	if err != nil {
		return 0, err
	}

	level.Debug(logger).Log(
		"message", "Latest schema fetched from registry",
		"type", s.Type,
		"latestSchema", latestSchema.Schema)

	if version != latestSchema.Version {
		level.Warn(logger).Log(
			"message", "The schema from the schema registry has a different version from the compiled schema",
			"compiled_version", version, "schema_registry_version", latestSchema.Version)
	}

	schema, err := client.GetSchemaBySubject(s.Subject, version)
	if err != nil {
		return 0, err
	}

	level.Debug(logger).Log(
		"message", "Current schema fetched from registry",
		"type", s.Type,
		"currentSchema", schema.Schema)

	return schema.Id, nil

}
