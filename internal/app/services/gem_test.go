package services

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGemReportPostsExceptionEvent(t *testing.T) {
	var captured GemReportRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/exception-event" {
			t.Fatalf("path = %s, want /exception-event", r.URL.Path)
		}
		if got := r.Header.Get(CorrelationIDHeader); got != "run-1" {
			t.Fatalf("correlation header = %q, want run-1", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exceptionEventId":"event-1","correlationId":"run-1"}`))
	}))
	defer server.Close()

	client := NewGemClient(server.URL+"/", true)
	response, err := client.Report(context.Background(), buildTradeRefreshGemRequest(
		"run-1",
		ApiException,
		"Aladdin trade refresh failed",
		errors.New("gateway timeout"),
		"phase=fetch_trades",
	))
	if err != nil {
		t.Fatalf("Report returned error: %v", err)
	}
	if response == nil || response.ExceptionEventID != "event-1" || response.CorrelationID != "run-1" {
		t.Fatalf("unexpected GEM response: %#v", response)
	}
	if captured.CorrelationID != "run-1" ||
		captured.ExceptionTypeName != "Technical_Exception" ||
		captured.ExceptionSeverityName != "High" ||
		captured.ExceptionName != ApiException ||
		captured.DataSource != "Aladdin Trade API" ||
		captured.DataTarget != "Trade Sink - Snowflake" ||
		captured.BusinessWorkflowName != "Trade Refresh" ||
		captured.TransactorServiceName != "Trade Refresher" ||
		captured.TransactorFunctionName != "refresh" ||
		captured.ProcessingPayload != "phase=fetch_trades" {
		t.Fatalf("unexpected GEM request: %#v", captured)
	}
	if !strings.Contains(captured.ExceptionMessage, "gateway timeout") || !strings.Contains(captured.StackTrace, "gateway timeout") {
		t.Fatalf("GEM request did not include useful error details: %#v", captured)
	}
	if captured.ActualExceptionDateTime == "" || strings.HasSuffix(captured.ActualExceptionDateTime, "Z") {
		t.Fatalf("GEM actual exception date should be a Pacific timestamp with offset: %#v", captured)
	}
}

func TestGemBuilderUsesPacificOffsets(t *testing.T) {
	tests := []struct {
		name string
		time time.Time
		want string
	}{
		{name: "PDT", time: time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC), want: "2026-07-08T09:00:00-07:00"},
		{name: "PST", time: time.Date(2026, 1, 8, 16, 0, 0, 0, time.UTC), want: "2026-01-08T08:00:00-08:00"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := NewGemRequestBuilder("run-1").WithException().ActualDate(test.time).Build()
			if request.ActualExceptionDateTime != test.want {
				t.Fatalf("actualExceptionDateTime = %q, want %q", request.ActualExceptionDateTime, test.want)
			}
		})
	}
}

func TestGemReportDisabledDoesNotPost(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer server.Close()

	client := NewGemClient(server.URL, false)
	response, err := client.Report(context.Background(), buildTradeRefreshGemRequest(
		"run-1",
		DatabaseConnectionException,
		"Snowflake commit failed",
		errors.New("connection failed"),
		"phase=commit_rows",
	))
	if err != nil {
		t.Fatalf("Report returned error: %v", err)
	}
	if response != nil {
		t.Fatalf("response = %#v, want nil", response)
	}
	if called {
		t.Fatal("disabled GEM client should not post")
	}
}

func TestGemReportRejectsNonSuccessStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"message":"bad gateway","details":"not logged wholesale"}`))
	}))
	defer server.Close()

	client := NewGemClient(server.URL, true)
	_, err := client.Report(context.Background(), buildTradeRefreshGemRequest(
		"run-1",
		ApiException,
		"Aladdin trade refresh failed",
		errors.New("gateway timeout"),
		"phase=fetch_trades",
	))
	if err == nil {
		t.Fatal("Report returned nil error for non-success status")
	}
}
