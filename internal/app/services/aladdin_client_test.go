package services

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/time/rate"

	logutil "refresher/trade-refresher/internal/utils/log"
)

func TestRequestQueryForLogRedactsPageToken(t *testing.T) {
	body := []byte(`{
		"pageToken": "opaque-token-value",
		"pageSize": 1000,
		"query": {"criteria": {
			"portfolio": {"portfolioReferences": [{"portfolioTicker": "702T"}]},
			"dateTime": {"modifyTimeRange": {
				"startTime": "2026-07-08T11:00:00Z",
				"endTime": "2026-07-08T11:03:00Z"
			}}
		}}
	}`)

	got := requestQueryForLog(OperationFilterTrades, body)
	if strings.Contains(got, "opaque-token-value") {
		t.Fatalf("requestQueryForLog leaked page token: %s", got)
	}
	if !strings.Contains(got, `"pageTokenPresent":true`) || !strings.Contains(got, `"pageTokenHash"`) {
		t.Fatalf("requestQueryForLog did not retain page token presence/hash metadata: %s", got)
	}
	if !strings.Contains(got, `"portfolioTicker":"702T"`) {
		t.Fatalf("requestQueryForLog removed useful query criteria: %s", got)
	}
	if !strings.Contains(got, `"startTime":"2026-07-08T04:00:00-07:00"`) || strings.Contains(got, `"startTime":"2026-07-08T11:00:00Z"`) {
		t.Fatalf("requestQueryForLog did not convert logged query time to Pacific: %s", got)
	}
}

func TestAladdinClientRetriesReadsButNotWrites(t *testing.T) {
	var readCalls atomic.Int32
	var writeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/trades:filter":
			if readCalls.Add(1) == 1 {
				response.WriteHeader(http.StatusGatewayTimeout)
				return
			}
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write([]byte(`{"blockTrades":[]}`))
		case "/trades:post":
			writeCalls.Add(1)
			response.WriteHeader(http.StatusGatewayTimeout)
		}
	}))
	defer server.Close()

	client := NewAladdinClient(AladdinClientConfig{
		BaseURL:            server.URL,
		RetryReadAttempts:  2,
		RetryWriteAttempts: 4,
		RetryBaseDelay:     time.Millisecond,
		RetryMaxDelay:      time.Millisecond,
		APIEnabled:         true,
	}, nil, NewSharedLimiter(RateLimitConfig{}))
	readResponse, err := client.Do(context.Background(), OperationFilterTrades, nil, []byte(`{}`), "read-run")
	if err != nil || readResponse.StatusCode != http.StatusOK || readCalls.Load() != 2 {
		t.Fatalf("read retry failed: response=%#v calls=%d err=%v", readResponse, readCalls.Load(), err)
	}
	writeResponse, err := client.Do(context.Background(), OperationPostTrade, nil, []byte(`{}`), "write-run")
	if err != nil || writeResponse.StatusCode != http.StatusGatewayTimeout || !writeResponse.UnknownOutcome || writeCalls.Load() != 1 {
		t.Fatalf("write should not retry: response=%#v calls=%d err=%v", writeResponse, writeCalls.Load(), err)
	}
}

func TestAladdinClientLogsSanitizedErrorForRetriedGatewayTimeout(t *testing.T) {
	previousLogger := logutil.Logger
	core, observed := observer.New(zap.DebugLevel)
	logutil.Logger = zap.New(core)
	t.Cleanup(func() { logutil.Logger = previousLogger })

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			response.Header().Set(BlackRockRequestIDHeader, "blackrock-504-request")
			response.Header().Set("Content-Type", "application/json")
			response.WriteHeader(http.StatusGatewayTimeout)
			_, _ = response.Write([]byte(`{"code":"GATEWAY_TIMEOUT","message":"BlackRock gateway timed out","error":{"rawTradePayload":"do-not-log"}}`))
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"blockTrades":[]}`))
	}))
	defer server.Close()

	client := NewAladdinClient(AladdinClientConfig{
		BaseURL:           server.URL,
		RetryReadAttempts: 2,
		RetryBaseDelay:    time.Millisecond,
		RetryMaxDelay:     time.Millisecond,
		APIEnabled:        true,
	}, nil, NewSharedLimiter(RateLimitConfig{}))
	requestCtx := withPortfolioGroupLogContext(context.Background(), "TCW_ALL")
	response, err := client.Do(requestCtx, OperationFilterTrades, nil, []byte(`{
		"query":{"criteria":{"portfolio":{"portfolioReferences":[
			{"portfolioId":"101","portfolioTicker":"702T"},
			{"portfolioId":"102","portfolioTicker":"710T"}
		]}}}
	}`), "team@example.com")
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("Do response=%#v error=%v", response, err)
	}

	entries := observed.FilterMessage("blackrock.request.completed").All()
	if len(entries) != 2 {
		t.Fatalf("completed log entries = %d, want 2", len(entries))
	}
	fields := entries[0].ContextMap()
	encodedFields := fmt.Sprint(fields)
	for _, expected := range []string{
		"team@example.com",
		"blackrock-504-request",
		"GATEWAY_TIMEOUT",
		"BlackRock gateway timed out",
		"portfolioId",
	} {
		if !strings.Contains(encodedFields, expected) {
			t.Fatalf("504 log fields %v do not contain %q", fields, expected)
		}
	}
	if strings.Contains(encodedFields, "do-not-log") || strings.Contains(encodedFields, "rawTradePayload") {
		t.Fatalf("504 log fields leaked upstream structured error data: %v", fields)
	}
	if got := fmt.Sprint(fields["status_code"]); got != "504" {
		t.Fatalf("status_code = %s, want 504", got)
	}
	for key, want := range map[string]string{
		"portfolio_group":    "TCW_ALL",
		"portfolio_id":       "101,102",
		"portfolio_number":   "702T,710T",
		"portfolio_count":    "2",
		"blackrock_endpoint": "/trades:filter",
	} {
		if got := fmt.Sprint(fields[key]); got != want {
			t.Fatalf("%s = %q, want %q (fields=%v)", key, got, want, fields)
		}
	}
}

func TestAladdinClientBoundsInFlightResponses(t *testing.T) {
	client := NewAladdinClient(AladdinClientConfig{APIEnabled: true}, nil, NewSharedLimiter(RateLimitConfig{}))
	for range maxConcurrentAladdinRequests {
		if err := client.acquireRequest(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.acquireRequest(ctx); err != context.DeadlineExceeded {
		t.Fatalf("acquireRequest error = %v, want deadline exceeded", err)
	}
	for range maxConcurrentAladdinRequests {
		client.releaseRequest()
	}
}

func TestSharedLimiterClampsOpenAPIQuotas(t *testing.T) {
	limiter := NewSharedLimiter(RateLimitConfig{
		ReadPerMinute:  2000,
		WritePerMinute: 500,
		ReadBurst:      2000,
		WriteBurst:     500,
	})
	if got := limiter.read.Limit(); got != rate.Limit(maxAladdinReadsPerMinute)/60 {
		t.Fatalf("read rate = %v", got)
	}
	if got := limiter.write.Limit(); got != rate.Limit(maxAladdinWritesPerMinute)/60 {
		t.Fatalf("write rate = %v", got)
	}
	if limiter.read.Burst() != maxAladdinReadsPerMinute || limiter.write.Burst() != maxAladdinWritesPerMinute {
		t.Fatalf("bursts = read %d, write %d", limiter.read.Burst(), limiter.write.Burst())
	}
}

func TestSharedLimiterReportsConfiguredQuota(t *testing.T) {
	limiter := NewSharedLimiter(RateLimitConfig{ReadPerMinute: 120, WritePerMinute: 30})
	if got := limiter.LimitPerMinute(QuotaRead, 1000); got != 120 {
		t.Fatalf("reported read quota = %d, want 120", got)
	}
	if got := limiter.LimitPerMinute(QuotaWrite, 250); got != 30 {
		t.Fatalf("reported write quota = %d, want 30", got)
	}
}

func TestRequestQueryForLogRedactsWritePayload(t *testing.T) {
	got := requestQueryForLog(OperationPostTrade, []byte(`{"clientSecret":"do-not-log"}`))
	if strings.Contains(got, "do-not-log") || strings.Contains(got, "clientSecret") {
		t.Fatalf("requestQueryForLog leaked write payload: %s", got)
	}
	if !strings.Contains(got, `"redacted":true`) || !strings.Contains(got, `"payloadBytes"`) {
		t.Fatalf("requestQueryForLog did not report redacted write payload metadata: %s", got)
	}
}

func TestUpstreamErrorSummaryAllowlistsFields(t *testing.T) {
	got := upstreamErrorSummary([]byte(`{
		"message": "gateway timeout",
		"error": {"rawTradePayload": "also-do-not-log"},
		"details": {"rawTradePayload": "do-not-log"}
	}`))
	if strings.Contains(got, "do-not-log") || strings.Contains(got, "also-do-not-log") || strings.Contains(got, "rawTradePayload") {
		t.Fatalf("upstreamErrorSummary leaked non-allowlisted details: %s", got)
	}
	if !strings.Contains(got, "gateway timeout") {
		t.Fatalf("upstreamErrorSummary removed useful message: %s", got)
	}
}
