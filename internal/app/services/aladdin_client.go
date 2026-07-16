package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"refresher/trade-refresher/internal/utils/azure"
	"refresher/trade-refresher/internal/utils/log"
)

const (
	maxAladdinResponseBytes      = 64 << 20
	maxConcurrentAladdinRequests = 4
	blackRockTimestampFormat     = "2006-01-02T15:04:05Z"
)

type AladdinClientConfig struct {
	BaseURL                string
	TradePath              string
	PortfolioGroupPath     string
	OAuthEnabled           bool
	TokenURL               string
	Scopes                 []string
	ClientIDSecretName     string
	ClientSecretSecretName string
	CredentialsTTL         time.Duration
	Timeout                time.Duration
	RetryReadAttempts      int
	RetryWriteAttempts     int
	RetryBaseDelay         time.Duration
	RetryMaxDelay          time.Duration
	APIEnabled             bool
}

type AladdinResponse struct {
	StatusCode                 int
	Body                       []byte
	Headers                    http.Header
	BlackRockRequestID         string
	BlackRockResponseID        string
	BlackRockResponseTimestamp string
	Attempts                   int
	UnknownOutcome             bool
}

type AladdinClient struct {
	cfg      AladdinClientConfig
	vault    azure.Vault
	limiter  *SharedLimiter
	client   *http.Client
	requests chan struct{}
	tokenMu  sync.Mutex
	token    *oauth2.Token
	tokenExp time.Time
}

type portfolioGroupLogContextKey struct{}

func withPortfolioGroupLogContext(ctx context.Context, portfolioGroup string) context.Context {
	portfolioGroup = strings.TrimSpace(portfolioGroup)
	if portfolioGroup == "" {
		return ctx
	}
	return context.WithValue(ctx, portfolioGroupLogContextKey{}, portfolioGroup)
}

func NewAladdinClient(cfg AladdinClientConfig, vault azure.Vault, limiter *SharedLimiter) *AladdinClient {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	return &AladdinClient{
		cfg:      cfg,
		vault:    vault,
		limiter:  limiter,
		requests: make(chan struct{}, maxConcurrentAladdinRequests),
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *AladdinClient) Do(ctx context.Context, op TradeOperation, pathParams map[string]string, body []byte, correlationID string) (*AladdinResponse, error) {
	if !c.cfg.APIEnabled {
		return nil, errors.New("aladdin api is disabled")
	}
	if correlationID == "" {
		correlationID = uuid.NewString()
	}

	maxAttempts := c.maxAttempts(op)
	var lastErr error
	var lastResponse *AladdinResponse
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.limiter.Wait(ctx, op.QuotaFamily); err != nil {
			return nil, err
		}

		requestID := uuid.NewString()
		req, upstreamPath, err := c.newRequest(ctx, op, pathParams, body, requestID, correlationID)
		if err != nil {
			return nil, err
		}
		requestQuery := requestQueryForLog(op, body)
		callFields := aladdinCallLogFields(ctx, upstreamPath, pathParams, body)
		if err := c.acquireRequest(ctx); err != nil {
			return nil, err
		}

		start := time.Now()
		startedFields := []zap.Field{
			zap.String("correlation_id", correlationID),
			zap.String("operation_id", op.Name),
			zap.String("http_method", op.Method),
			zap.Int("attempt", attempt),
			zap.String("blackrock_request_id", requestID),
			zap.String("request_query", requestQuery),
		}
		startedFields = append(startedFields, callFields...)
		log.Logger.Debug("blackrock.request.started", startedFields...)

		resp, err := c.client.Do(req)
		elapsed := time.Since(start)
		if err != nil {
			c.releaseRequest()
			lastErr = err
			unknown := op.QuotaFamily == QuotaWrite
			failedFields := []zap.Field{
				zap.String("correlation_id", correlationID),
				zap.String("operation_id", op.Name),
				zap.String("http_method", op.Method),
				zap.String("blackrock_request_id", requestID),
				zap.Int("attempt", attempt),
				zap.Int64("duration_ms", elapsed.Milliseconds()),
				zap.String("request_query", requestQuery),
				zap.Error(err),
			}
			if unknown {
				failedFields = append(failedFields, zap.Bool("unknown_outcome", true))
			}
			failedFields = append(failedFields, callFields...)
			log.Logger.Warn("blackrock.request.failed", failedFields...)
			if unknown || attempt == maxAttempts {
				return &AladdinResponse{
					BlackRockRequestID: requestID,
					Attempts:           attempt,
					UnknownOutcome:     unknown,
				}, err
			}
			c.sleepBeforeRetry(ctx, attempt, 0)
			continue
		}

		respBody, readErr := readAndClose(resp.Body)
		c.releaseRequest()
		if readErr != nil {
			lastErr = readErr
		}
		responseRequestID := resp.Header.Get(BlackRockRequestIDHeader)
		if responseRequestID == "" {
			responseRequestID = requestID
		}
		lastResponse = &AladdinResponse{
			StatusCode:                 resp.StatusCode,
			Body:                       respBody,
			Headers:                    resp.Header.Clone(),
			BlackRockRequestID:         responseRequestID,
			BlackRockResponseID:        resp.Header.Get(BlackRockResponseIDHeader),
			BlackRockResponseTimestamp: resp.Header.Get(BlackRockResponseTimeHeader),
			Attempts:                   attempt,
			UnknownOutcome:             op.QuotaFamily == QuotaWrite && (isUnknownOutcomeStatus(resp.StatusCode) || readErr != nil),
		}

		fields := []zap.Field{
			zap.String("correlation_id", correlationID),
			zap.String("operation_id", op.Name),
			zap.String("http_method", op.Method),
			zap.Int("attempt", attempt),
			zap.Int("status_code", resp.StatusCode),
			zap.Int("response_bytes", len(respBody)),
			zap.Int64("duration_ms", elapsed.Milliseconds()),
			zap.String("blackrock_request_id", responseRequestID),
			zap.String("blackrock_response_id", lastResponse.BlackRockResponseID),
			zap.String("blackrock_response_timestamp_pt", pacificHeaderTimestamp(lastResponse.BlackRockResponseTimestamp)),
			zap.String("request_query", requestQuery),
		}
		fields = append(fields, callFields...)
		if responseRequestID != requestID {
			fields = append(fields, zap.Bool("request_id_mismatch", true))
		}
		if shouldRetry(op, resp.StatusCode) && attempt < maxAttempts {
			fields = append(fields, zap.Bool("retryable", true))
		}
		if lastResponse.UnknownOutcome {
			fields = append(fields, zap.Bool("unknown_outcome", true))
		}
		if resp.StatusCode >= 400 {
			fields = append(fields, zap.String("error_summary", upstreamErrorSummary(respBody)))
		}
		if readErr != nil {
			fields = append(fields, zap.Error(readErr))
			log.Logger.Warn("blackrock.request.completed", fields...)
			if op.QuotaFamily == QuotaRead && attempt < maxAttempts {
				c.sleepBeforeRetry(ctx, attempt, 0)
				continue
			}
			return lastResponse, readErr
		}
		lastErr = nil

		if resp.StatusCode == http.StatusUnauthorized && attempt == 1 {
			log.Logger.Warn("blackrock.request.completed", fields...)
			c.clearToken()
			continue
		}
		if !shouldRetry(op, resp.StatusCode) || attempt == maxAttempts {
			if resp.StatusCode >= 400 {
				log.Logger.Warn("blackrock.request.completed", fields...)
			} else {
				log.Logger.Info("blackrock.request.completed", fields...)
			}
			return lastResponse, lastErr
		}
		log.Logger.Warn("blackrock.request.completed", fields...)
		delay := retryAfter(resp.Header)
		if resp.StatusCode == http.StatusTooManyRequests && delay <= 0 {
			delay = time.Minute
		}
		c.sleepBeforeRetry(ctx, attempt, delay)
	}
	return lastResponse, lastErr
}

func aladdinCallLogFields(ctx context.Context, upstreamPath string, pathParams map[string]string, body []byte) []zap.Field {
	metadata := portfolioLogMetadata{}
	if group, ok := ctx.Value(portfolioGroupLogContextKey{}).(string); ok {
		metadata.addGroup(group)
	}
	if pathParams != nil {
		metadata.addGroup(pathParams["portfolioGroup"])
	}
	if len(body) > 0 {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		if decoder.Decode(&value) == nil {
			metadata.collect(value)
		}
	}
	fields := []zap.Field{zap.String("blackrock_endpoint", upstreamPath)}
	if len(metadata.groups) > 0 {
		fields = append(fields, zap.String("portfolio_group", strings.Join(metadata.groups, ",")))
	}
	if len(metadata.ids) > 0 {
		fields = append(fields, zap.String("portfolio_id", strings.Join(metadata.ids, ",")))
	}
	if len(metadata.tickers) > 0 {
		fields = append(fields, zap.String("portfolio_number", strings.Join(metadata.tickers, ",")))
	}
	if count := max(len(metadata.ids), len(metadata.tickers)); count > 0 {
		fields = append(fields, zap.Int("portfolio_count", count))
	}
	return fields
}

type portfolioLogMetadata struct {
	groups    []string
	ids       []string
	tickers   []string
	groupSet  map[string]struct{}
	idSet     map[string]struct{}
	tickerSet map[string]struct{}
}

func (m *portfolioLogMetadata) collect(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			switch key {
			case "portfolioGroupTicker":
				m.addGroup(logScalar(child))
			case "portfolioId":
				m.addID(logScalar(child))
			case "portfolioTicker":
				m.addTicker(logScalar(child))
			}
			m.collect(child)
		}
	case []any:
		for _, child := range typed {
			m.collect(child)
		}
	}
}

func (m *portfolioLogMetadata) addGroup(value string) {
	m.groups, m.groupSet = appendUniqueLogValue(m.groups, m.groupSet, value)
}

func (m *portfolioLogMetadata) addID(value string) {
	m.ids, m.idSet = appendUniqueLogValue(m.ids, m.idSet, value)
}

func (m *portfolioLogMetadata) addTicker(value string) {
	m.tickers, m.tickerSet = appendUniqueLogValue(m.tickers, m.tickerSet, value)
}

func appendUniqueLogValue(values []string, seen map[string]struct{}, value string) ([]string, map[string]struct{}) {
	value = strings.TrimSpace(value)
	if value == "" || len(values) >= maxPortfolioReferencesPerTradeFilter {
		return values, seen
	}
	if seen == nil {
		seen = make(map[string]struct{})
	}
	key := strings.ToUpper(value)
	if _, exists := seen[key]; exists {
		return values, seen
	}
	seen[key] = struct{}{}
	return append(values, value), seen
}

func logScalar(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return fmt.Sprint(typed)
	default:
		return ""
	}
}

func (c *AladdinClient) acquireRequest(ctx context.Context) error {
	select {
	case c.requests <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *AladdinClient) releaseRequest() {
	<-c.requests
}

func (c *AladdinClient) newRequest(ctx context.Context, op TradeOperation, pathParams map[string]string, body []byte, requestID, correlationID string) (*http.Request, string, error) {
	path := op.UpstreamPath
	for key, value := range pathParams {
		path = strings.ReplaceAll(path, "{"+key+"}", url.PathEscape(value))
	}
	requestPath := strings.TrimRight(c.cfg.TradePath, "/") + path
	if op.FromAPIBase {
		requestPath = path
	}
	fullURL := strings.TrimRight(c.cfg.BaseURL, "/") + "/" + strings.Trim(requestPath, "/")

	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, op.Method, fullURL, reader)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(BlackRockRequestIDHeader, requestID)
	req.Header.Set(BlackRockOriginTimestampHeader, time.Now().UTC().Format(blackRockTimestampFormat))
	req.Header.Set(CorrelationIDHeader, correlationID)
	if c.cfg.OAuthEnabled {
		token, err := c.getToken(ctx)
		if err != nil {
			return nil, "", err
		}
		req.Header.Set(AuthorizationHeader, "Bearer "+token.AccessToken)
	}
	return req, path, nil
}

func (c *AladdinClient) getToken(ctx context.Context) (*oauth2.Token, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	now := time.Now()
	if c.token != nil && c.token.Valid() && now.Before(c.tokenExp) {
		return c.token, nil
	}
	if c.vault == nil {
		return nil, errors.New("key vault is required for Aladdin OAuth")
	}

	clientID, err := c.vault.Get(c.cfg.ClientIDSecretName)
	if err != nil {
		return nil, fmt.Errorf("unable to load Aladdin OAuth client id: %w", err)
	}
	clientSecret, err := c.vault.Get(c.cfg.ClientSecretSecretName)
	if err != nil {
		return nil, fmt.Errorf("unable to load Aladdin OAuth client secret: %w", err)
	}
	oauthConfig := clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     c.cfg.TokenURL,
		Scopes:       c.cfg.Scopes,
	}
	tokenSource := oauthConfig.TokenSource(ctx)
	token, err := tokenSource.Token()
	if err != nil {
		return nil, err
	}
	c.token = token
	ttl := c.cfg.CredentialsTTL
	if ttl <= 0 {
		ttl = 8 * time.Hour
	}
	c.tokenExp = now.Add(ttl)
	return token, nil
}

func (c *AladdinClient) clearToken() {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	c.token = nil
	c.tokenExp = time.Time{}
}

func (c *AladdinClient) maxAttempts(op TradeOperation) int {
	if op.QuotaFamily == QuotaWrite {
		if c.cfg.RetryWriteAttempts <= 0 {
			return 1
		}
		return c.cfg.RetryWriteAttempts
	}
	if c.cfg.RetryReadAttempts <= 0 {
		return 6
	}
	return c.cfg.RetryReadAttempts
}

func (c *AladdinClient) sleepBeforeRetry(ctx context.Context, attempt int, retryAfter time.Duration) {
	delay := retryAfter
	if delay <= 0 {
		base := c.cfg.RetryBaseDelay
		if base <= 0 {
			base = 500 * time.Millisecond
		}
		maxDelay := c.cfg.RetryMaxDelay
		if maxDelay <= 0 {
			maxDelay = 30 * time.Second
		}
		delay = base * time.Duration(1<<(attempt-1))
		if delay > maxDelay {
			delay = maxDelay
		}
		delay += time.Duration(rand.Int63n(int64(base)))
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func shouldRetry(op TradeOperation, status int) bool {
	if op.QuotaFamily == QuotaWrite {
		return false
	}
	switch status {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func isUnknownOutcomeStatus(status int) bool {
	return status == http.StatusGatewayTimeout || status == http.StatusBadGateway || status == http.StatusServiceUnavailable
}

func retryAfter(header http.Header) time.Duration {
	value := header.Get("Retry-After")
	if value == "" {
		return 0
	}
	if seconds, err := time.ParseDuration(value + "s"); err == nil {
		return seconds
	}
	if when, err := http.ParseTime(value); err == nil {
		return time.Until(when)
	}
	return 0
}

func pacificHeaderTimestamp(value string) string {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return pacificLogTimestamp(parsed)
	}
	if parsed, err := http.ParseTime(value); err == nil {
		return pacificLogTimestamp(parsed)
	}
	if value == "" {
		return ""
	}
	return "unparseable"
}

func readAndClose(body io.ReadCloser) ([]byte, error) {
	defer body.Close()
	payload, err := io.ReadAll(io.LimitReader(body, maxAladdinResponseBytes+1))
	if err != nil {
		return payload, err
	}
	if len(payload) > maxAladdinResponseBytes {
		return payload[:maxAladdinResponseBytes], fmt.Errorf("aladdin response exceeded %d bytes", maxAladdinResponseBytes)
	}
	return payload, nil
}

func requestQueryForLog(op TradeOperation, body []byte) string {
	if len(body) == 0 {
		return "{}"
	}
	if op.QuotaFamily == QuotaWrite {
		return fmt.Sprintf(`{"redacted":true,"payloadBytes":%d}`, len(body))
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return fmt.Sprintf(`{"invalidJson":true,"payloadBytes":%d}`, len(body))
	}
	redactPageToken(value)
	convertLogTimesToPacific(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf(`{"unmarshalable":true,"payloadBytes":%d}`, len(body))
	}
	return truncateLogJSON(encoded)
}

func redactPageToken(value any) {
	object, ok := value.(map[string]any)
	if !ok {
		return
	}
	if token, ok := object["pageToken"].(string); ok && token != "" {
		object["pageTokenPresent"] = true
		object["pageTokenHash"] = hashParts(token)[:12]
		delete(object, "pageToken")
	}
	for _, child := range object {
		redactPageToken(child)
	}
}

func convertLogTimesToPacific(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if text, ok := child.(string); ok && (key == "startTime" || key == "endTime") {
				if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
					typed[key] = pacificLogTimestamp(parsed)
					continue
				}
			}
			convertLogTimesToPacific(child)
		}
	case []any:
		for _, child := range typed {
			convertLogTimesToPacific(child)
		}
	}
}

func upstreamErrorSummary(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		return fmt.Sprintf("non-json upstream error body, bytes=%d", len(body))
	}
	allowed := map[string]any{}
	for _, key := range []string{"code", "status", "message", "error", "details"} {
		if value, ok := object[key]; ok {
			allowed[key] = safeErrorLogValue(value, 0)
		}
	}
	if len(allowed) == 0 {
		return fmt.Sprintf("json upstream error body, bytes=%d", len(body))
	}
	encoded, err := json.Marshal(allowed)
	if err != nil {
		return fmt.Sprintf("json upstream error body, bytes=%d", len(body))
	}
	return truncateLogJSON(encoded)
}

func safeErrorLogValue(value any, depth int) any {
	if depth > 3 {
		return "[nested value omitted]"
	}
	switch typed := value.(type) {
	case string, float64, bool, nil:
		return redactLongValue(typed)
	case map[string]any:
		allowed := make(map[string]any)
		for _, key := range []string{"code", "status", "message", "reason", "field", "description", "error", "details"} {
			if child, ok := typed[key]; ok {
				allowed[key] = safeErrorLogValue(child, depth+1)
			}
		}
		if len(allowed) == 0 {
			return "[structured value omitted]"
		}
		return allowed
	case []any:
		limit := min(len(typed), 10)
		values := make([]any, 0, limit)
		for _, child := range typed[:limit] {
			values = append(values, safeErrorLogValue(child, depth+1))
		}
		return values
	default:
		return "[value omitted]"
	}
}

func redactLongValue(value any) any {
	var text string
	switch typed := value.(type) {
	case string:
		text = typed
	case float64, bool, nil:
		return value
	default:
		return "[redacted structured value]"
	}
	if len(text) <= 256 {
		return text
	}
	return text[:256] + "...[truncated]"
}

func truncateLogJSON(body []byte) string {
	const max = 4096
	if len(body) > max {
		body = body[:max]
	}
	return string(body)
}