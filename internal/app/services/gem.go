package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"refresher/trade-refresher/internal/utils/log"
)

type ExceptionNameEnum string

const (
	ApiException                 ExceptionNameEnum = "ApiException"
	DatabaseConnectionException  ExceptionNameEnum = "DatabaseConnectionException"
	IllegalStateException        ExceptionNameEnum = "IllegalStateException"
	AuthorizationFailedException ExceptionNameEnum = "AuthorizationFailedException"
)

type GemReportRequest struct {
	CorrelationID            string            `json:"correlationId"`
	ExceptionTypeName        string            `json:"exceptionTypeName"`
	ExceptionSeverityName    string            `json:"exceptionSeverityName"`
	DataSource               string            `json:"dataSource"`
	DataTarget               string            `json:"dataTarget"`
	BusinessWorkflowName     string            `json:"businessWorkflowName"`
	ActualExceptionDateTime  string            `json:"actualExceptionDateTime"`
	ExceptionName            ExceptionNameEnum `json:"exceptionName"`
	ExceptionMessage         string            `json:"exceptionMessage"`
	TransactorServiceName    string            `json:"transactorServiceName"`
	TransactorServiceVersion string            `json:"transactorServiceVersion"`
	TransactorFunctionName   string            `json:"transactorFunctionName"`
	TransactorUserName       string            `json:"transactorUserName"`
	TransactorComputerName   string            `json:"transactorComputerName"`
	StackTrace               string            `json:"stackTrace"`
	ProcessingPayload        string            `json:"processingPayload"`
}

type GemReportResponse struct {
	ExceptionEventID string `json:"exceptionEventId"`
	CorrelationID    string `json:"correlationId"`
}

type GemClient struct {
	baseURL string
	enabled bool
	client  *http.Client
}

func NewGemClient(baseURL string, enabled bool) *GemClient {
	return &GemClient{
		baseURL: baseURL,
		enabled: enabled,
		client:  &http.Client{Timeout: 15 * time.Second},
	}
}

func (g *GemClient) Report(ctx context.Context, request *GemReportRequest) (*GemReportResponse, error) {
	if g == nil || !g.enabled {
		return nil, nil
	}
	if request == nil {
		return nil, errors.New("gem request cannot be nil")
	}
	if strings.TrimSpace(g.baseURL) == "" {
		return nil, errors.New("gem url is not configured")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal gem request: %w", err)
	}
	endpoint := strings.TrimRight(g.baseURL, "/") + "/exception-event"
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create gem request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	if request.CorrelationID != "" {
		httpRequest.Header.Set(CorrelationIDHeader, request.CorrelationID)
	}

	start := time.Now()
	response, err := g.client.Do(httpRequest)
	if err != nil {
		log.Logger.Error("gem.report.failed",
			zap.String("correlation_id", request.CorrelationID),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err),
		)
		return nil, err
	}
	defer response.Body.Close()

	respBytes, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
	if readErr != nil {
		err := fmt.Errorf("read gem response: %w", readErr)
		log.Logger.Error("gem.report.failed",
			zap.String("correlation_id", request.CorrelationID),
			zap.Int("status_code", response.StatusCode),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err),
		)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		err := fmt.Errorf("gem returned non-success status %d", response.StatusCode)
		log.Logger.Error("gem.report.failed",
			zap.String("correlation_id", request.CorrelationID),
			zap.Int("status_code", response.StatusCode),
			zap.Duration("duration", time.Since(start)),
			zap.String("response_summary", gemResponseSummary(respBytes)),
			zap.Error(err),
		)
		return nil, err
	}

	var gemResponse GemReportResponse
	if err := json.Unmarshal(respBytes, &gemResponse); err != nil {
		err = fmt.Errorf("decode gem response: %w", err)
		log.Logger.Error("gem.report.failed",
			zap.String("correlation_id", request.CorrelationID),
			zap.Int("status_code", response.StatusCode),
			zap.Duration("duration", time.Since(start)),
			zap.String("response_summary", gemResponseSummary(respBytes)),
			zap.Error(err),
		)
		return nil, err
	}
	if strings.TrimSpace(gemResponse.ExceptionEventID) == "" {
		err := errors.New("gem response is missing exceptionEventId")
		log.Logger.Error("gem.report.failed",
			zap.String("correlation_id", request.CorrelationID),
			zap.Int("status_code", response.StatusCode),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err),
		)
		return nil, err
	}

	log.Logger.Info("gem.report.succeeded",
		zap.String("correlation_id", request.CorrelationID),
		zap.String("exception_event_id", gemResponse.ExceptionEventID),
		zap.Int("status_code", response.StatusCode),
		zap.Duration("duration", time.Since(start)),
	)
	return &gemResponse, nil
}

func gemResponseSummary(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		return fmt.Sprintf("non-json GEM response body, bytes=%d", len(body))
	}
	allowed := map[string]any{}
	for _, key := range []string{"code", "status", "message", "error", "correlationId", "exceptionEventId"} {
		if value, ok := object[key]; ok {
			allowed[key] = redactLongValue(value)
		}
	}
	if len(allowed) == 0 {
		return fmt.Sprintf("json GEM response body, bytes=%d", len(body))
	}
	encoded, err := json.Marshal(allowed)
	if err != nil {
		return fmt.Sprintf("json GEM response body, bytes=%d", len(body))
	}
	return truncateLogJSON(encoded)
}
