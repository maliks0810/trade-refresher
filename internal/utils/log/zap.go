package log

import (
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"refresher/trade-refresher/internal/utils/ptime"
)

var logger *zap.Logger
var once sync.Once

var Logger = instance()

func instance() *zap.Logger {
	once.Do(func() {
		logger = create()
	})

	return logger
}

func create() *zap.Logger {
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeTime = pacificTimeEncoder
	l, err := config.Build()
	if err != nil {
		return zap.NewNop()
	}
	return l
}

func pacificTimeEncoder(value time.Time, encoder zapcore.PrimitiveArrayEncoder) {
	encoder.AppendString(ptime.PacificLogTimestamp(value))
}
