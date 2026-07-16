package net

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	appLog "refresher/trade-refresher/internal/utils/log"
)

func StartServerWithGracefulShutdown(a *fiber.App, stopBackground func()) {
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- a.Listen(":8100")
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	stop := sync.OnceFunc(func() {
		if stopBackground != nil {
			stopBackground()
		}
	})
	defer stop()

	select {
	case signal := <-signals:
		appLog.Logger.Info("fiber.server.shutdown_started", zap.String("signal", signal.String()))
		stop()
		if err := a.ShutdownWithTimeout(20 * time.Second); err != nil {
			appLog.Logger.Error("fiber.server.shutdown_failed", zap.Error(err))
		}
		select {
		case err := <-serverErr:
			if err != nil {
				appLog.Logger.Error("fiber.server.listen_failed", zap.Error(err))
			}
		case <-time.After(time.Second):
			appLog.Logger.Warn("fiber.server.stop_timeout")
		}
	case err := <-serverErr:
		if err != nil {
			appLog.Logger.Fatal("fiber.server.listen_failed", zap.Error(err))
		}
	}
}
