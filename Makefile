test:
	go test $$(go list ./... | grep -v /vendor/)
