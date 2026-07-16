package routes

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"refresher/trade-refresher/internal/utils/log"
)

func UtilityRoutes(ctx context.Context, app *fiber.App) {
	registerHealthRoutes(app)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("status: healthy"))
	})
	srv := &http.Server{
		Addr:              ":8080",
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Hour,
		Handler:           mux,
	}
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()
	go func() {
		select {
		case err := <-serverErr:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Logger.Error("health.server.listen_failed", zap.Error(err))
			}
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				log.Logger.Error("health.server.shutdown_failed", zap.Error(err))
			}
		}
	}()
}

func registerHealthRoutes(app *fiber.App) {
	app.Get(apiBasePath+"/live", healthHandler)
	app.Get(apiBasePath+"/ready", healthHandler)
}

func healthHandler(ctx *fiber.Ctx) error {
	return ctx.JSON(fiber.Map{"status": "healthy"})
}
