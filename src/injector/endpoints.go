package injector

import (
	"context"

	"bitbucket.org/ubeedev/kakfa-elasticsearch-injector-go/src/kafka"
	"github.com/go-kit/kit/endpoint"
)

type Endpoints struct {
	Insert endpoint.Endpoint
}

func MakeInsertEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		records := request.([]*kafka.Record)

		return nil, svc.Insert(records)
	}
}
