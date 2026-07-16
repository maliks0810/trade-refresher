package routes

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"refresher/trade-refresher/internal/app/handlers"
	"refresher/trade-refresher/internal/app/services"
	"refresher/trade-refresher/internal/middleware"
	logutil "refresher/trade-refresher/internal/utils/log"
)

func TestTradeRoutesRequireAPIKey(t *testing.T) {
	app := fiber.New()
	auth := services.NewAPIKeyAuthenticator(nil, "", time.Minute, "test-key")
	TradeRoutes(app, handlers.NewTradeHandler(nil), auth)

	request := httptest.NewRequest(http.MethodPost, "/de/v1/api/trades:filter", strings.NewReader(`{}`))
	request.Header.Set(services.CorrelationIDHeader, "investment-operations")
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusUnauthorized)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"correlationId":"investment-operations"`) {
		t.Fatalf("authentication error does not return correlation ID: %s", body)
	}
}

func TestTradeRouteLogsCorrelationWithoutAPIKey(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	originalLogger := logutil.Logger
	logutil.Logger = zap.New(core)
	t.Cleanup(func() { logutil.Logger = originalLogger })

	app := fiber.New()
	middleware.FiberMiddleware(app)
	auth := services.NewAPIKeyAuthenticator(nil, "", time.Minute, "never-log-this-api-key")
	TradeRoutes(app, handlers.NewTradeHandler(nil), auth)

	request := httptest.NewRequest(http.MethodPost, "/de/v1/api/trades:filter", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer never-log-this-api-key")
	request.Header.Set(services.CorrelationIDHeader, "investment-operations")
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}

	entries := observed.All()
	if len(entries) == 0 {
		t.Fatal("request produced no structured log entry")
	}
	for _, entry := range entries {
		encoded := entry.ContextMap()
		if strings.Contains(entry.Message, "never-log-this-api-key") {
			t.Fatal("API key leaked in log message")
		}
		for _, value := range encoded {
			if strings.Contains(fmt.Sprint(value), "never-log-this-api-key") {
				t.Fatal("API key leaked in structured log fields")
			}
		}
	}
	if got := entries[len(entries)-1].ContextMap()["correlation_id"]; got != "investment-operations" {
		t.Fatalf("logged correlation ID = %v, want investment-operations", got)
	}
}

func TestHealthRoutesRemainUnauthenticated(t *testing.T) {
	app := fiber.New()
	registerHealthRoutes(app)
	auth := services.NewAPIKeyAuthenticator(nil, "", time.Minute, "test-key")
	TradeRoutes(app, handlers.NewTradeHandler(nil), auth)

	for _, path := range []string{"/de/v1/api/live", "/de/v1/api/ready"} {
		response, err := app.Test(httptest.NewRequest(http.MethodGet, path, nil))
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", path, response.StatusCode, http.StatusOK)
		}
	}
}

func TestTradeRoutesAcceptCaseInsensitiveBearerScheme(t *testing.T) {
	app := fiber.New()
	auth := services.NewAPIKeyAuthenticator(nil, "", time.Minute, "test-key")
	TradeRoutes(app, handlers.NewTradeHandler(nil), auth)

	request := httptest.NewRequest(http.MethodPost, "/de/v1/api/trades:filter", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "bearer test-key")
	request.Header.Set(services.CorrelationIDHeader, "investment-operations")
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want authenticated request to reach nil handler", response.StatusCode)
	}
}

func TestTradeRoutesRequireCorrelationID(t *testing.T) {
	app := fiber.New()
	auth := services.NewAPIKeyAuthenticator(nil, "", time.Minute, "test-key")
	TradeRoutes(app, handlers.NewTradeHandler(nil), auth)

	request := httptest.NewRequest(http.MethodPost, "/de/v1/api/trades:filter", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer test-key")
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
}

func TestTradeRoutesRejectLongCorrelationID(t *testing.T) {
	app := fiber.New()
	auth := services.NewAPIKeyAuthenticator(nil, "", time.Minute, "test-key")
	TradeRoutes(app, handlers.NewTradeHandler(nil), auth)

	request := httptest.NewRequest(http.MethodPost, "/de/v1/api/trades:filter", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer test-key")
	request.Header.Set(services.CorrelationIDHeader, strings.Repeat("a", correlationIDMaxLength+1))
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("app.Test returned error: %v", err)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
}

func TestTradeRoutesRegisterAllOpenAPIEndpoints(t *testing.T) {
	app := fiber.New()
	auth := services.NewAPIKeyAuthenticator(nil, "", time.Minute, "test-key")
	TradeRoutes(app, handlers.NewTradeHandler(nil), auth)

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/de/v1/api/longrunningoperations/op-1"},
		{http.MethodPost, "/de/v1/api/trades:filter"},
		{http.MethodPost, "/de/v1/api/trades:retrieve"},
		{http.MethodPost, "/de/v1/api/trades:post"},
		{http.MethodPost, "/de/v1/api/trades:batchPost"},
		{http.MethodPost, "/de/v1/api/trades:cancel"},
		{http.MethodPost, "/de/v1/api/trades:batchCancel"},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(`{}`))
			request.Header.Set("Authorization", "Bearer test-key")
			request.Header.Set(services.CorrelationIDHeader, "investment-operations")
			request.Header.Set("Content-Type", "application/json")
			response, err := app.Test(request)
			if err != nil {
				t.Fatalf("app.Test returned error: %v", err)
			}
			if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusMethodNotAllowed {
				t.Fatalf("route was not registered, status = %d", response.StatusCode)
			}
			if response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want service unavailable from nil test client", response.StatusCode)
			}
		})
	}
}
