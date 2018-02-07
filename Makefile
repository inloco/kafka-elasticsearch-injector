go/deps:
	dep ensure -v

test:
	go test $$(go list ./... | grep -v /vendor/)
