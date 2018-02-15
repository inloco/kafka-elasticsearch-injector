package probes

import (
	"net/http"
	"os"
)

var (
	LivenessRoute  = os.Getenv("K8S_LIVENESS_ROUTE")
	ReadinessRoute = os.Getenv("K8S_READINESS_ROUTE")
)

type ProbeCheck func() bool

type Probes struct {
	livenessCheck  ProbeCheck
	readinessCheck ProbeCheck
	port           string
}

func New(port string) *Probes {
	return &Probes{
		port: port,
		livenessCheck: func() bool {
			return false
		},
		readinessCheck: func() bool {
			return false
		},
	}
}

func (p *Probes) SetLivenessCheck(fn ProbeCheck) {
	p.livenessCheck = fn
}

func (p *Probes) Dead() {
	p.livenessCheck = func() bool {
		return false
	}
}

func (p *Probes) Alive() {
	p.livenessCheck = func() bool {
		return true
	}
}

func (p *Probes) SetReadinessCheck(fn ProbeCheck) {
	p.readinessCheck = fn
}

func (p *Probes) Unready() {
	p.readinessCheck = func() bool {
		return false
	}
}

func (p *Probes) Ready() {
	p.readinessCheck = func() bool {
		return true
	}
}

func (p *Probes) Serve() error {
	mux := http.NewServeMux()

	mux.Handle(LivenessRoute, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !p.livenessCheck() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))

	mux.Handle(ReadinessRoute, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !p.readinessCheck() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))

	return http.ListenAndServe(":"+p.port, mux)
}
