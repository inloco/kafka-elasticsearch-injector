package metrics_instrumenter

import (
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func Register() {
	http.Handle("/metrics", promhttp.Handler())
	port := os.Getenv("METRICS_PORT")
	go http.ListenAndServe(":"+port, nil)
}
