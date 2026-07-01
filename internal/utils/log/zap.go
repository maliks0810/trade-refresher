package log

import (
	"sync"
	"go.uber.org/zap"
)

var logger *zap.Logger
var once sync.Once

var Logger = instance()

// Instance function returns an instance of the Uber Zap logger.  Will only ever create a single
// instance of the logger
func instance() *zap.Logger {
	once.Do(func() {
		logger = create()
	})

	return logger
}

func create() *zap.Logger {
	l, _ := zap.NewProduction()
	return l
}
