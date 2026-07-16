package middleware

import (
	"runtime/debug"
	"strings"
	"time"

	"github.com/gofiber/contrib/otelfiber/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"refresher/trade-refresher/internal/utils/log"
)

func FiberMiddleware(application *fiber.App) {
	application.Use(
		otelfiber.Middleware(),
		correlationID(),
		compress.New(compressionConfig()),
		requestLogger(),
		recover.New(recoverConfig()),
	)
}

func compressionConfig() compress.Config {
	return compress.Config{
		Level: compress.LevelBestSpeed,
	}

}

func correlationID() fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		correlationID := strings.TrimSpace(ctx.Get("X-Correlation-ID"))
		if correlationID == "" {
			correlationID = strings.TrimSpace(ctx.Get("X-Request-ID"))
		}
		if correlationID == "" || len(correlationID) > 256 {
			correlationID = uuid.NewString()
		}
		ctx.Locals("correlation_id", correlationID)
		ctx.Set("X-Correlation-ID", correlationID)
		return ctx.Next()
	}
}

func requestLogger() fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		start := time.Now()
		err := ctx.Next()
		route := ctx.Path()
		if ctx.Route() != nil && ctx.Route().Path != "" {
			route = ctx.Route().Path
		}
		statusCode := ctx.Response().StatusCode()
		if err != nil && statusCode < fiber.StatusBadRequest {
			statusCode = fiber.StatusInternalServerError
		}
		fields := []zap.Field{
			zap.String("http.request.method", ctx.Method()),
			zap.String("http.route", route),
			zap.Int("http.request.body.size", len(ctx.Request().Body())),
			zap.Int("http.response.status_code", statusCode),
			zap.Int("http.response.body.size", len(ctx.Response().Body())),
			zap.Int64("http.server.request.duration_ms", time.Since(start).Milliseconds()),
			zap.String("client_ip", ctx.IP()),
		}
		if operationID, ok := ctx.Locals("operation_id").(string); ok && operationID != "" {
			fields = append(fields, zap.String("operation_id", operationID))
		}
		if correlationID, ok := ctx.Locals("correlation_id").(string); ok && correlationID != "" {
			fields = append(fields, zap.String("correlation_id", correlationID))
		}
		if fingerprint, ok := ctx.Locals("api_key_fingerprint").(string); ok && fingerprint != "" {
			fields = append(fields, zap.String("api_key_fingerprint", fingerprint))
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			log.Logger.Error("http.request.failed", fields...)
			return err
		}
		if statusCode >= fiber.StatusInternalServerError {
			log.Logger.Error("http.request.completed", fields...)
		} else if statusCode >= fiber.StatusBadRequest {
			log.Logger.Warn("http.request.completed", fields...)
		} else {
			log.Logger.Info("http.request.completed", fields...)
		}
		return nil
	}
}

func recoverConfig() recover.Config {
	return recover.Config{
		EnableStackTrace: true,
		StackTraceHandler: func(ctx *fiber.Ctx, recovered any) {
			correlationID, _ := ctx.Locals("correlation_id").(string)
			log.Logger.Error("http.request.panic",
				zap.String("correlation_id", correlationID),
				zap.Any("panic", recovered),
				zap.ByteString("stack_trace", debug.Stack()),
			)
		},
	}
}
