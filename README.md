# Kafka elasticsearch injector
[![CircleCI](https://circleci.com/gh/inloco/kafka-elasticsearch-injector.svg?style=svg&circle-token=9b8a15af1d6bd1fde69dc0fbc37ff191f4c95473)](https://circleci.com/gh/inloco/kafka-elasticsearch-injector)

Application responsible for loading kafka topics into elasticsearch. Some use cases:

- Using elasticsearch as a debugging tool to monitor data activity in kafka topics
- Using elasticsearch + kibana as an analytics tool
- Easily integrating applications with elasticsearch

## Usage

To create new injectors for your topics, you should create a new kubernetes deployment with your configurations.

### Configuration variables
- `KAFKA_ADDRESS` Kafka url. **REQUIRED**
- `SCHEMA_REGISTRY_URL` Schema registry url port and protocol. **REQUIRED**
- `KAFKA_TOPICS` Comma separated list of kafka topics to subscribe **REQUIRED**
- `KAFKA_CONSUMER_GROUP` Consumer group id, should be unique across the cluster. Please be careful with this variable **REQUIRED**
- `ELASTICSEARCH_HOST` Elasticsearch url with port and protocol. **REQUIRED**
- `ES_INDEX` Elasticsearch index prefix to write records to(actual index is followed by the record's timestamp to avoid very large indexes). Defaults to topic name. **OPTIONAL**
- `PROBES_PORT` Kubernetes probes port. Set to any available port. **REQUIRED**
- `K8S_LIVENESS_ROUTE` Kubernetes route for liveness check. **REQUIRED**
- `K8S_READINESS_ROUTE`Kubernetes route for readiness check. **REQUIRED**
- `KAFKA_CONSUMER_CONCURRENCY` Number of parallel goroutines working as a consumer. Default value is 1 **OPTIONAL**
- `KAFKA_CONSUMER_BATCH_SIZE` Number of records to accumulate before sending them to elasticsearch(for each goroutine). Default value is 100 **OPTIONAL**
- `ES_INDEX_COLUMN` Record field to append to index name. Ex: to create one ES index per campaign, use "campaign_id" here **OPTIONAL**
- `ES_BLACKLISTED_COLUMNS` Comma separated list of record fields to filter before sending to elasticsearch. Defaults to empty string. **OPTIONAL**
- `ES_DOC_ID_COLUMN` Record field to be the document ID of Elasticsearch. Defaults to "kafkaRecordPartition:kafkaRecordOffset". **OPTIONAL**
- `LOG_LEVEL` Determines the log level for the app. Should be set to DEBUG, WARN, NONE or INFO. Defaults to INFO. **OPTIONAL**
- `METRICS_PORT` Port to export app metrics **REQUIRED**
- `ES_BULK_TIMEOUT` Timeout for elasticsearch bulk writes in the format of golang's `time.ParseDuration`. Default value is 1s **OPTIONAL**
- `ES_BULK_BACKOFF` Constant backoff when elasticsearch is overloaded. in the format of golang's `time.ParseDuration`. Default value is 1s **OPTIONAL**
- `ES_TIME_SUFFIX` Indicates what time unit to append to index names on elasticsearch. Supported values are `day` and `hour`. Default value is `day` **OPTIONAL**
- `KAFKA_CONSUMER_RECORD_TYPE` Kafka record type. Should be set to "avro" or "json". Defaults to avro. **OPTIONAL**
- `KAFKA_CONSUMER_METRICS_UPDATE_INTERVAL` The interval which the app updates the exported metrics in the format of golang's `time.ParseDuration`. Defaults to 30s. **OPTIONAL**

### Important note about Elasticsearch mappings and types

As you may know, Elasticsearch is capable of mapping inference. In other words, it'll try to guess
your mappings based on the kind of data you are sending. This is fine for some use cases, but we
strongly recommend that you create your own mappings (Especially if you care about your date
types). If you are using multiple indexes, a index template is something that you should look into.

If you are planning on using kibana as an analytics tool, is recommended to use a template for your data like belows.

### Setting up a template in Elastic Search

> Note: This step only works with elastic search 5.5.0 and above.

Index templates allow you to define templates that will automatically be applied when new indices are created. In this
example, a wildcard (*) is used and every new index following this pattern will use the template configuration.

To set a template for some index,  send a PUT REST method to: ```http://elasticsearch:9200/_template/sample-logs ``` with belows JSON
file.

```json
{
  "template": "sample-logs-*",
  "mappings": {
    "_default_": {
      "_all": {
        "enabled": "false"
      },
      "_source": {
        "enabled": "true"
      },
      "properties": {
        "timestamp": {
          "type": "date",
          "format": "date_optional_time",
          "ignore_malformed": true
        }
      },
      "dynamic_templates": [
        {
          "strings": {
            "match_mapping_type": "string",
            "mapping": {
              "type": "text",
              "index": false
            }
          }
        }
      ]
    }
  }
}
```

This template does the following

- Makes all strings not analyzed by default. If you want/need analyzed fields, you can either remove
  the dynamic template for "strings" or add your field as a property
- Adds a date property to be used as a time field. Replace according to your field name and format.
- Sets the `_default_` type. Matches all types and acts as a base. You can override settings or set
  new properties by adding other types.
- If your're using a timestamp with milliseconds, use ```epoch_millis``` as format for date type.

You can find more information here: [Indices Templates][indices_templates], [Mapping Changes][mapping_changes],
[Date datatype][date_datatype]

### Monitoring

This project exports metrics such as consumer lag by default. We don't have a grafana template ready for consumers but you can use existing
dashboards as inspiration for your application.

The exported metrics are:
- `kafka_consumer_partition_delay`: number of records betweeen last record consumed successfully and the last record on kafka, by partition and topic.
- `kafka_consumer_records_consumed_successfully`: number of records consumed successfully by this instance.
- `kafka_consumer_endpoint_latency_histogram_seconds`: endpoint latency in seconds (insertion to elasticsearch).
- `kafka_consumer_buffer_full`: indicates whether the app buffer is full(meaning that elasticsearch is not being able to keep up with the topic volume).

## Development

This repository uses Go modules for dependency management, make sure it is enabled.
To build the project from source, run from project root:
```bash
go build cmd/injector.go
```

To run tests, run `docker-compose up -d zookeeper kafka schema-registry elasticsearch` and run `make test`. 

### Versioning

The project's version is kept on the `VERSION` file on the project's root dir. 
Please update this file before merging any PRs to the master branch.

When a `git push` is triggered, CircleCI will run the project's tests and push the generated docker image to dockerhub.
If the current branch is master, the docker tag will be `<version>`. If not, it will be `<version>-<commit-sha>`

[mapping_changes]: https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking_50_mapping_changes.html
[indices_templates]: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html
[date_datatype]: https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
