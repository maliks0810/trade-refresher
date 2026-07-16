package routes

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"refresher/trade-refresher/internal/app/handlers"
	"refresher/trade-refresher/internal/app/services"
	"refresher/trade-refresher/internal/utils/log"
)

func TradeRoutes(app *fiber.App, tradeHandler *handlers.TradeHandler, auth *services.APIKeyAuthenticator) {
	route := app.Group(apiBasePath, requireCorrelationID(), apiKeyMiddleware(auth))

	route.Get("/longrunningoperations/:id", tradeHandler.LongRunningOperation)
	route.Post("/trades:filter", tradeHandler.FilterTrades)
	route.Post("/trades:retrieve", tradeHandler.RetrieveTrades)
	route.Post("/trades:post", tradeHandler.PostTrade)
	route.Post("/trades:batchPost", tradeHandler.BatchPostTrades)
	route.Post("/trades:cancel", tradeHandler.CancelTrade)
	route.Post("/trades:batchCancel", tradeHandler.BatchCancelTrades)
}

func requireCorrelationID() fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		correlationID := strings.TrimSpace(ctx.Get(services.CorrelationIDHeader))
		if correlationID == "" {
			requestID, _ := ctx.Locals("correlation_id").(string)
			log.Logger.Warn("api.correlation.failure",
				zap.String("reason", "missing"),
				zap.String("request_id", requestID),
				zap.String("http.request.method", ctx.Method()),
				zap.String("http.route", ctx.Path()),
			)
			return sendRouteError(ctx, fiber.StatusBadRequest, "missing X-Correlation-ID header", requestID)
		}
		if len(correlationID) > correlationIDMaxLength {
			requestID, _ := ctx.Locals("correlation_id").(string)
			log.Logger.Warn("api.correlation.failure",
				zap.String("reason", "too_long"),
				zap.Int("correlation_id_length", len(correlationID)),
				zap.String("http.request.method", ctx.Method()),
				zap.String("http.route", ctx.Path()),
			)
			return sendRouteError(ctx, fiber.StatusBadRequest, "X-Correlation-ID header exceeds 256 characters", requestID)
		}
		ctx.Locals("correlation_id", correlationID)
		ctx.Set(services.CorrelationIDHeader, correlationID)
		return ctx.Next()
	}
}

func apiKeyMiddleware(auth *services.APIKeyAuthenticator) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		correlationID, _ := ctx.Locals("correlation_id").(string)
		if auth == nil {
			log.Logger.Error("api.auth.failure", zap.String("correlation_id", correlationID), zap.String("reason", "not_configured"))
			return sendRouteError(ctx, fiber.StatusServiceUnavailable, "api key authentication is not configured", correlationID)
		}
		scheme, provided, found := strings.Cut(strings.TrimSpace(ctx.Get(fiber.HeaderAuthorization)), " ")
		provided = strings.TrimSpace(provided)
		if !found || !strings.EqualFold(scheme, "Bearer") || provided == "" {
			log.Logger.Warn("api.auth.failure", zap.String("correlation_id", correlationID), zap.String("reason", "missing_or_malformed"))
			return sendRouteError(ctx, fiber.StatusUnauthorized, "missing or malformed authorization header", correlationID)
		}
		ok, keyFingerprint, err := auth.Validate(provided)
		if err != nil {
			log.Logger.Error("api.auth.failure", zap.String("correlation_id", correlationID), zap.Error(err))
			return sendRouteError(ctx, fiber.StatusServiceUnavailable, "api key authentication is unavailable", correlationID)
		}
		if !ok {
			log.Logger.Warn("api.auth.failure", zap.String("correlation_id", correlationID), zap.String("reason", "invalid"))
			return sendRouteError(ctx, fiber.StatusUnauthorized, "invalid api key", correlationID)
		}
		ctx.Locals("api_key_fingerprint", keyFingerprint)
		return ctx.Next()
	}
}

func sendRouteError(ctx *fiber.Ctx, status int, message, correlationID string) error {
	return ctx.Status(status).JSON(fiber.Map{
		"error":         message,
		"correlationId": correlationID,
	})
}
