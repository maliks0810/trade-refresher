package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"refresher/trade-refresher/internal/app/services"
	"refresher/trade-refresher/internal/utils/log"
)

type TradeHandler struct {
	Client *services.AladdinClient
}

func NewTradeHandler(client *services.AladdinClient) *TradeHandler {
	return &TradeHandler{Client: client}
}

func (h *TradeHandler) LongRunningOperation(ctx *fiber.Ctx) error {
	return h.proxy(ctx, services.OperationGetLongRunningOperation, map[string]string{"id": ctx.Params("id")})
}

func (h *TradeHandler) FilterTrades(ctx *fiber.Ctx) error {
	return h.proxy(ctx, services.OperationFilterTrades, nil)
}

func (h *TradeHandler) RetrieveTrades(ctx *fiber.Ctx) error {
	return h.proxy(ctx, services.OperationRetrieveTrades, nil)
}

func (h *TradeHandler) PostTrade(ctx *fiber.Ctx) error {
	return h.proxy(ctx, services.OperationPostTrade, nil)
}

func (h *TradeHandler) BatchPostTrades(ctx *fiber.Ctx) error {
	return h.proxy(ctx, services.OperationBatchPostTrades, nil)
}

func (h *TradeHandler) CancelTrade(ctx *fiber.Ctx) error {
	return h.proxy(ctx, services.OperationCancelTrade, nil)
}

func (h *TradeHandler) BatchCancelTrades(ctx *fiber.Ctx) error {
	return h.proxy(ctx, services.OperationBatchCancelTrades, nil)
}

func (h *TradeHandler) proxy(ctx *fiber.Ctx, op services.TradeOperation, pathParams map[string]string) error {
	ctx.Locals("operation_id", op.Name)
	if h == nil || h.Client == nil {
		return ctx.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "trade proxy is not configured"})
	}
	correlationID, _ := ctx.Locals("correlation_id").(string)

	body := ctx.Body()
	if op.Method == http.MethodPost && len(body) > 0 && !json.Valid(body) {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid json body"})
	}

	response, err := h.Client.Do(ctx.UserContext(), op, pathParams, body, correlationID)
	if err != nil {
		unknownOutcome := response != nil && response.UnknownOutcome
		blackRockRequestID := ""
		attempts := 0
		if response != nil {
			blackRockRequestID = response.BlackRockRequestID
			attempts = response.Attempts
			ctx.Set(services.BlackRockRequestIDHeader, blackRockRequestID)
		}
		log.Logger.Error("api.proxy.failed",
			zap.String("correlation_id", correlationID),
			zap.String("operation_id", op.Name),
			zap.String("blackrock_request_id", blackRockRequestID),
			zap.Int("attempts", attempts),
			zap.Bool("unknown_outcome", unknownOutcome),
			zap.Error(err),
		)
		if unknownOutcome {
			ctx.Set("X-Trade-Unknown-Outcome", "true")
		}
		return ctx.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error":              "upstream request failed",
			"correlationId":      correlationID,
			"blackRockRequestId": blackRockRequestID,
			"unknownOutcome":     unknownOutcome,
		})
	}
	if response == nil {
		return ctx.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": "empty upstream response", "correlationId": correlationID})
	}
	ctx.Set(services.CorrelationIDHeader, correlationID)
	ctx.Set(services.BlackRockRequestIDHeader, response.BlackRockRequestID)
	ctx.Set(services.BlackRockResponseIDHeader, response.BlackRockResponseID)
	ctx.Set(services.BlackRockResponseTimeHeader, response.BlackRockResponseTimestamp)
	if response.UnknownOutcome {
		ctx.Set("X-Trade-Unknown-Outcome", "true")
	}
	for _, header := range []string{"Content-Type"} {
		if value := response.Headers.Get(header); value != "" {
			ctx.Set(header, value)
		}
	}
	if ctx.GetRespHeader("Content-Type") == "" {
		ctx.Set("Content-Type", "application/json")
	}
	return ctx.Status(response.StatusCode).Send(response.Body)
}
