package injector

import (
	"context"

	"bitbucket.org/ubeedev/kafka-elasticsearch-injector-go/src/models"
	"github.com/go-kit/kit/endpoint"
)

type basicEndpoints struct {
	insertEndpoint endpoint.Endpoint
}

func (be *basicEndpoints) Insert() endpoint.Endpoint {
	return be.insertEndpoint
}

type Endpoints interface {
	Insert() endpoint.Endpoint
}

func MakeEndpoints(svc Service) Endpoints {
	return &basicEndpoints{
		insertEndpoint: makeInsertEndpoint(svc),
	}
}

func makeInsertEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		records := request.([]*models.Record)

		return nil, svc.Insert(records)
	}
}
