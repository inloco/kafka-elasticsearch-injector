package logger_builder

import (
	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

func NewLogger(service string) (logger log.Logger) {
	logger = log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	logger = level.NewFilter(logger, allowedLevels())
	logger = log.With(logger, "caller", log.DefaultCaller)
	logger = log.With(logger, "time", log.DefaultTimestampUTC)
	logger = log.With(logger, "service", service)

	return
}

func allowedLevels() level.Option {
	switch config := os.Getenv("LOG_LEVEL"); {
	case config == "DEBUG":
		return level.AllowDebug()
	case config == "WARN":
		return level.AllowWarn()
	case config == "NONE":
		return level.AllowNone()
	default:
		return level.AllowInfo()
	}
}
