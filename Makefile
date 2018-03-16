go/deps:
	dep ensure -v

test:
	go test $$(go list ./... | grep -v /vendor/)

docker/build:
	GOOS=linux GOARCH=386 go build -o bin/injector cmd/injector.go
	GOOS=linux GOARCH=386 go build -o bin/producer util/producer/producer.go
	docker build --rm=false -t "inlocomedia/kafka-elasticsearch-injector:local" -f cmd/Dockerfile .
	docker build --rm=false -t "inlocomedia/kafka-elasticsearch-injector:producer-local" -f util/producer/Dockerfile .

docker/run:
	docker-compose up -d zookeeper kafka schema-registry elasticsearch kibana
	count=0; \
          until curl localhost:9200 || ((count ++ >= 10)); \
          do echo "Retrying: Verify if Elasticsearch is ready"; sleep 5; done
	curl -XPOST "localhost:9200/_template/my-topic" --data '{"template":"my-topic-*","settings":{"refresh_interval":"30s","number_of_replicas":0},"mappings":{"_default_":{"_all":{"enabled":"false"},"_source":{"enabled":"true"},"properties":{"@timestamp":{"format":"epoch_millis","ignore_malformed":true,"type":"date"}},"dynamic_templates":[{"strings":{"match_mapping_type":"string","mapping":{"type":"text","index":false}}}]}}}'
	docker-compose up -d producer app