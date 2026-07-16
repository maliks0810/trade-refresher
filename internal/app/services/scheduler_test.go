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

func TestFetchTradePagesFollowsEveryPageToken(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestCount++
		var body struct {
			PageToken string `json:"pageToken"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		response.Header().Set("Content-Type", "application/json")
		if requestCount == 1 {
			if body.PageToken != "" {
				t.Errorf("first page token = %q", body.PageToken)
			}
			_, _ = response.Write([]byte(`{"blockTrades":[{"trades":[{"id":"trade-1"}]}],"nextPageToken":"page-2"}`))
			return
		}
		if body.PageToken != "page-2" {
			t.Errorf("second page token = %q, want page-2", body.PageToken)
		}
		_, _ = response.Write([]byte(`{"blockTrades":[{"trades":[{"id":"trade-2"}]}]}`))
	}))
	defer server.Close()

	client := NewAladdinClient(AladdinClientConfig{BaseURL: server.URL, APIEnabled: true}, nil, NewSharedLimiter(RateLimitConfig{}))
	scheduler := &TradeScheduler{
		client: client,
		cfg: SchedulerConfig{
			PageSize:        1000,
			DecodeWorkers:   2,
			PortfolioFilter: PortfolioFilter{Type: PortfolioFilterGroup, Values: []string{"TCW_ALL"}, Raw: "group:TCW_ALL"},
		},
	}
	dataset := NewTradeDataset()
	summary, err := scheduler.fetchTradePages(context.Background(), "run-1", time.Now().Add(-time.Minute), time.Now(), func(_ int, page TradeDataset) (MergePageResult, error) {
		dataset = dataset.Append(page)
		return MergePageResult{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.PageCount != 2 || requestCount != 2 || dataset.TotalRows() != 2 {
		t.Fatalf("pages=%d requests=%d rows=%d, want 2/2/2", summary.PageCount, requestCount, dataset.TotalRows())
	}
}

func TestFetchTradeWindowsSplitsCatchupIntoMinimumContiguousOneHourRequests(t *testing.T) {
	type requestRange struct {
		Start time.Time
		End   time.Time
	}
	ranges := make([]requestRange, 0, 25)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var body struct {
			Query struct {
				Criteria struct {
					DateTime struct {
						ModifyTimeRange struct {
							StartTime time.Time `json:"startTime"`
							EndTime   time.Time `json:"endTime"`
						} `json:"modifyTimeRange"`
					} `json:"dateTime"`
				} `json:"criteria"`
			} `json:"query"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		rangeFilter := body.Query.Criteria.DateTime.ModifyTimeRange
		ranges = append(ranges, requestRange{Start: rangeFilter.StartTime, End: rangeFilter.EndTime})
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"blockTrades":[]}`))
	}))
	defer server.Close()

	client := NewAladdinClient(AladdinClientConfig{BaseURL: server.URL, APIEnabled: true}, nil, NewSharedLimiter(RateLimitConfig{}))
	scheduler := &TradeScheduler{
		client: client,
		cfg: SchedulerConfig{
			PageSize:        1000,
			DecodeWorkers:   2,
			PortfolioFilter: PortfolioFilter{Type: PortfolioFilterGroup, Values: []string{"TCW_ALL"}, Raw: "group:TCW_ALL"},
		},
	}
	start := time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC)
	end := start.Add(24*time.Hour + 5*time.Minute)
	pageNumbers := make([]int, 0, 25)
	summary, err := scheduler.fetchTradeWindows(context.Background(), "run-1", start, end, func(pageNumber int, _ TradeDataset) (MergePageResult, error) {
		pageNumbers = append(pageNumbers, pageNumber)
		return MergePageResult{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.QueryWindowCount != 25 || summary.PageCount != 25 || len(ranges) != 25 {
		t.Fatalf("windows=%d pages=%d requests=%d, want 25/25/25", summary.QueryWindowCount, summary.PageCount, len(ranges))
	}
	for index, requestRange := range ranges {
		if duration := requestRange.End.Sub(requestRange.Start); duration <= 0 || duration > time.Hour {
			t.Fatalf("request %d duration = %s, want (0, 1h]", index+1, duration)
		}
		if index == 0 && !requestRange.Start.Equal(start) {
			t.Fatalf("first request starts at %s, want %s", requestRange.Start, start)
		}
		if index > 0 {
			wantStart := ranges[index-1].End
			if !requestRange.Start.Equal(wantStart) {
				t.Fatalf("request %d starts at %s, want shared boundary %s", index+1, requestRange.Start, ranges[index-1].End)
			}
		}
		if pageNumbers[index] != index+1 {
			t.Fatalf("request %d page number = %d, want %d", index+1, pageNumbers[index], index+1)
		}
	}
	if !ranges[len(ranges)-1].End.Equal(end) {
		t.Fatalf("last request ends at %s, want %s", ranges[len(ranges)-1].End, end)
	}
}

func TestFetchTradePagesReturnsUsefulBlackRockValidationError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set(BlackRockRequestIDHeader, "blackrock-request-123")
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusBadRequest)
		_, _ = response.Write([]byte(`{"code":"VALIDATION_ERROR","message":"Please provide a valid modify time range duration within 3600 seconds."}`))
	}))
	defer server.Close()

	client := NewAladdinClient(AladdinClientConfig{BaseURL: server.URL, APIEnabled: true}, nil, NewSharedLimiter(RateLimitConfig{}))
	scheduler := &TradeScheduler{
		client: client,
		cfg: SchedulerConfig{
			PageSize:        1000,
			PortfolioFilter: PortfolioFilter{Type: PortfolioFilterGroup, Values: []string{"TCW_ALL"}, Raw: "group:TCW_ALL"},
		},
	}
	start := time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC)
	_, err := scheduler.fetchTradePages(context.Background(), "run-1", start, start.Add(time.Hour), nil)
	if err == nil {
		t.Fatal("fetchTradePages returned nil error")
	}
	for _, expected := range []string{
		"BlackRock /trades:filter failed",
		"status_code=400",
		"blackrock-request-123",
		"VALIDATION_ERROR",
		"within 3600 seconds",
		"modify_time_start_pt=2026-07-08T04:00:00-07:00",
		"modify_time_end_pt=2026-07-08T05:00:00-07:00",
	} {
		if !strings.Contains(err.Error(), expected) {
			t.Fatalf("error %q does not contain %q", err, expected)
		}
	}
}

func TestFetchTradePagesPreservesRequestIDForEmbeddedValidationError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set(BlackRockRequestIDHeader, "blackrock-embedded-123")
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"status":{"code":3,"message":"portfolio reference is invalid","details":[{"field":"portfolioId"}]},"blockTrades":[]}`))
	}))
	defer server.Close()

	scheduler := &TradeScheduler{
		client: NewAladdinClient(AladdinClientConfig{BaseURL: server.URL, APIEnabled: true}, nil, NewSharedLimiter(RateLimitConfig{})),
		cfg: SchedulerConfig{
			PageSize:        1000,
			PortfolioFilter: PortfolioFilter{Type: PortfolioFilterReferences, Values: []string{"REMOVED"}},
		},
	}
	start := time.Now().Add(-time.Minute)
	_, err := scheduler.fetchTradePages(context.Background(), "run-embedded", start, start.Add(time.Minute), nil)
	if err == nil {
		t.Fatal("fetchTradePages returned nil error")
	}
	for _, expected := range []string{"blackrock-embedded-123", "embedded_status_code=3", "portfolio reference is invalid"} {
		if !strings.Contains(err.Error(), expected) {
			t.Fatalf("error %q does not contain %q", err, expected)
		}
	}
}

func TestRetiredPortfolioFailureIsIsolatedAndQueuedWithoutFailingRefresh(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestCount++
		var body struct {
			Query struct {
				Criteria struct {
					Portfolio struct {
						References []PortfolioReference `json:"portfolioReferences"`
					} `json:"portfolio"`
				} `json:"criteria"`
			} `json:"query"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode filter request: %v", err)
		}
		for _, reference := range body.Query.Criteria.Portfolio.References {
			if reference.PortfolioID == "2" || reference.PortfolioTicker == "REMOVED" {
				response.Header().Set(BlackRockRequestIDHeader, "invalid-portfolio-request")
				response.WriteHeader(http.StatusBadRequest)
				_, _ = response.Write([]byte(`{"code":"VALIDATION_ERROR","message":"portfolio is inactive"}`))
				return
			}
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"status":{"code":200,"message":"Number of results processed successfully: 0/0"},"blockTrades":[]}`))
	}))
	defer server.Close()

	scheduler := &TradeScheduler{
		client: NewAladdinClient(AladdinClientConfig{BaseURL: server.URL, APIEnabled: true}, nil, NewSharedLimiter(RateLimitConfig{})),
		cfg: SchedulerConfig{
			PageSize:                      1000,
			PortfolioGroupRefreshInterval: 30 * time.Minute,
			PortfolioFilter:               PortfolioFilter{Type: PortfolioFilterGroup, Values: []string{"TCW_ALL"}},
		},
	}
	filter := portfolioMemberFilterBatches([]PortfolioReference{
		{PortfolioID: "1", PortfolioTicker: "VALID"},
		{PortfolioID: "2", PortfolioTicker: "REMOVED"},
	})[0]
	start := time.Now().Add(-3 * time.Minute)
	summary, err := scheduler.drainRetiredPortfolioFilter(context.Background(), "run-retired", start, start.Add(3*time.Minute), 0, filter, nil)
	if err != nil {
		t.Fatalf("removed portfolio should not fail the active refresh: %v", err)
	}
	if requestCount != 3 {
		t.Fatalf("filter request count = %d, want 3 (batch and two isolated IDs)", requestCount)
	}
	if len(summary.NewRecoveries) != 1 || summary.NewRecoveries[0].Reference.PortfolioID != "2" {
		t.Fatalf("queued recoveries = %#v, want only portfolio 2", summary.NewRecoveries)
	}
	if !summary.NewRecoveries[0].Start.Equal(start) || !summary.NewRecoveries[0].End.Equal(start.Add(3*time.Minute)) {
		t.Fatalf("recovery window = %s..%s", summary.NewRecoveries[0].Start, summary.NewRecoveries[0].End)
	}
}

func TestMostUsefulBatchErrorPrefersUpstreamFailure(t *testing.T) {
	upstream := &blackRockFilterError{StatusCode: http.StatusGatewayTimeout, RequestID: "request-504", Summary: "gateway timeout"}
	index, err := mostUsefulBatchError([]error{context.Canceled, upstream, context.Canceled}, context.Canceled)
	if index != 1 || !errors.Is(err, upstream) {
		t.Fatalf("selected batch=%d error=%v, want upstream batch 1", index, err)
	}
}

func TestFilterRequestBodyUsesUTCForOpenAPISpec(t *testing.T) {
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		PageSize: 1000,
		PortfolioFilter: PortfolioFilter{
			Type:   PortfolioFilterGroup,
			Values: []string{"TCW_ALL"},
			Raw:    "group:TCW_ALL",
		},
	}}

	body, err := scheduler.filterRequestBody(
		time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 8, 11, 3, 0, 0, time.UTC),
		"",
	)
	if err != nil {
		t.Fatalf("filterRequestBody returned error: %v", err)
	}

	var request struct {
		Query struct {
			Criteria struct {
				Portfolio struct {
					PortfolioGroupTicker string `json:"portfolioGroupTicker"`
				} `json:"portfolio"`
				DateTime struct {
					ModifyTimeRange struct {
						StartTime string `json:"startTime"`
						EndTime   string `json:"endTime"`
					} `json:"modifyTimeRange"`
				} `json:"dateTime"`
			} `json:"criteria"`
		} `json:"query"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}

	if request.Query.Criteria.Portfolio.PortfolioGroupTicker != "TCW_ALL" {
		t.Fatalf("portfolioGroupTicker = %q, want TCW_ALL", request.Query.Criteria.Portfolio.PortfolioGroupTicker)
	}
	rangeFilter := request.Query.Criteria.DateTime.ModifyTimeRange
	if rangeFilter.StartTime != "2026-07-08T11:00:00Z" {
		t.Fatalf("startTime = %q, want UTC timestamp", rangeFilter.StartTime)
	}
	if rangeFilter.EndTime != "2026-07-08T11:03:00Z" {
		t.Fatalf("endTime = %q, want UTC timestamp", rangeFilter.EndTime)
	}
}

func TestFilterRequestBodyRejectsRangeOverOneHour(t *testing.T) {
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		PageSize:        1000,
		PortfolioFilter: PortfolioFilter{Type: PortfolioFilterGroup, Values: []string{"TCW_ALL"}, Raw: "group:TCW_ALL"},
	}}
	start := time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC)
	_, err := scheduler.filterRequestBody(start, start.Add(time.Hour+time.Nanosecond), "")
	if err == nil || !strings.Contains(err.Error(), "no longer than 1h0m0s") {
		t.Fatalf("filterRequestBody error = %v, want one-hour range validation", err)
	}
}

func TestFilterRequestBodyUsesPortfolioReferences(t *testing.T) {
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		PageSize: 1000,
		PortfolioFilter: PortfolioFilter{
			Type:   PortfolioFilterReferences,
			Values: []string{"702T", "710T", "3409T"},
			Raw:    "702T,710T,3409T",
		},
	}}

	body, err := scheduler.filterRequestBody(
		time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 8, 11, 3, 0, 0, time.UTC),
		"",
	)
	if err != nil {
		t.Fatalf("filterRequestBody returned error: %v", err)
	}

	var request struct {
		Query struct {
			Criteria struct {
				Portfolio struct {
					PortfolioReferences []struct {
						PortfolioTicker string `json:"portfolioTicker"`
					} `json:"portfolioReferences"`
				} `json:"portfolio"`
			} `json:"criteria"`
		} `json:"query"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	references := request.Query.Criteria.Portfolio.PortfolioReferences
	if len(references) != 3 || references[0].PortfolioTicker != "702T" || references[1].PortfolioTicker != "710T" || references[2].PortfolioTicker != "3409T" {
		t.Fatalf("unexpected portfolio references: %#v", references)
	}
}

func TestNextRefreshWindowIncludesWeekendCatchup(t *testing.T) {
	location := pacificLocation()
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		Lookback:    5 * time.Minute,
		SafetyDelay: 60 * time.Second,
		MaxCatchup:  3 * 24 * time.Hour,
	}}
	now := time.Date(2026, 7, 13, 4, 3, 0, 0, location)
	checkpoint := CheckpointState{
		LastSuccessfulEnd: time.Date(2026, 7, 10, 18, 0, 0, 0, location),
	}

	window := scheduler.nextRefreshWindow(now, checkpoint)
	if !window.HasWork {
		t.Fatal("window.HasWork = false, want true")
	}
	if window.WasClipped {
		t.Fatal("weekend catch-up window should fit inside the three-day cap")
	}
	if got, want := pacificLogTimestamp(window.Start), "2026-07-10T17:55:00-07:00"; got != want {
		t.Fatalf("window start = %s, want %s", got, want)
	}
	if got, want := pacificLogTimestamp(window.End), "2026-07-13T04:02:00-07:00"; got != want {
		t.Fatalf("window end = %s, want %s", got, want)
	}
}

func TestNextRefreshWindowUsesConfiguredCatchupDuration(t *testing.T) {
	location := pacificLocation()
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		Lookback:    5 * time.Minute,
		SafetyDelay: 60 * time.Second,
		MaxCatchup:  7 * 24 * time.Hour,
	}}
	now := time.Date(2026, 7, 13, 4, 3, 0, 0, location)
	checkpoint := CheckpointState{
		LastSuccessfulEnd: time.Date(2026, 7, 1, 4, 0, 0, 0, location),
	}

	window := scheduler.nextRefreshWindow(now, checkpoint)
	if !window.HasWork {
		t.Fatal("window.HasWork = false, want true")
	}
	if !window.WasClipped {
		t.Fatal("window.WasClipped = false, want true")
	}
	if window.MaxCatchup != 7*24*time.Hour {
		t.Fatalf("max catch-up = %s, want 168h", window.MaxCatchup)
	}
	if got, want := pacificLogTimestamp(window.Start), "2026-07-06T04:02:00-07:00"; got != want {
		t.Fatalf("window start = %s, want %s", got, want)
	}
	if got, want := pacificLogTimestamp(window.End), "2026-07-13T04:02:00-07:00"; got != want {
		t.Fatalf("window end = %s, want %s", got, want)
	}
}

func TestNextRefreshWindowFirstRunUsesConfiguredDuration(t *testing.T) {
	location := pacificLocation()
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		Lookback:    5 * time.Minute,
		SafetyDelay: 60 * time.Second,
		MaxCatchup:  30 * 24 * time.Hour,
	}}
	now := time.Date(2026, 7, 13, 4, 3, 0, 0, location)

	window := scheduler.nextRefreshWindow(now, CheckpointState{})
	if !window.HasWork {
		t.Fatal("window.HasWork = false, want true")
	}
	if !window.FirstRun {
		t.Fatal("window.FirstRun = false, want true")
	}
	if window.WasClipped {
		t.Fatal("first run should start at the initial capped window, not clip an old checkpoint")
	}
	if got, want := pacificLogTimestamp(window.Start), "2026-06-13T04:02:00-07:00"; got != want {
		t.Fatalf("window start = %s, want %s", got, want)
	}
	if got, want := pacificLogTimestamp(window.End), "2026-07-13T04:02:00-07:00"; got != want {
		t.Fatalf("window end = %s, want %s", got, want)
	}
}

func TestNextRefreshWindowCatchupDurationIsExactAcrossPacificDST(t *testing.T) {
	location := pacificLocation()
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		SafetyDelay: time.Minute,
		MaxCatchup:  7 * 24 * time.Hour,
	}}
	now := time.Date(2026, 3, 10, 4, 1, 0, 0, location)

	window := scheduler.nextRefreshWindow(now, CheckpointState{})
	if got, want := pacificLogTimestamp(window.Start), "2026-03-03T03:00:00-08:00"; got != want {
		t.Fatalf("window start = %s, want %s", got, want)
	}
	if window.End.Sub(window.Start) != 168*time.Hour {
		t.Fatalf("DST-spanning seven-day window = %s, want 168h", window.End.Sub(window.Start))
	}
}

func TestBusinessWindowAlwaysUsesPacificTime(t *testing.T) {
	scheduler := &TradeScheduler{}
	tests := []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "summer opens at 4 PDT", now: time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC), want: true},
		{name: "summer before 4 PDT", now: time.Date(2026, 7, 8, 10, 59, 0, 0, time.UTC), want: false},
		{name: "winter opens at 4 PST", now: time.Date(2026, 1, 8, 12, 0, 0, 0, time.UTC), want: true},
		{name: "winter before 4 PST", now: time.Date(2026, 1, 8, 11, 59, 0, 0, time.UTC), want: false},
		{name: "closes at 6 PM PDT", now: time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := scheduler.inBusinessWindow(test.now); got != test.want {
				t.Fatalf("inBusinessWindow(%s) = %t, want %t", test.now, got, test.want)
			}
		})
	}
}

func TestBusinessWindowUsesConfiguredPacificTimes(t *testing.T) {
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		BusinessStartMinutePT: 6*60 + 30,
		BusinessEndMinutePT:   17*60 + 15,
	}}
	location := pacificLocation()
	for _, test := range []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "before", now: time.Date(2026, 7, 8, 6, 29, 0, 0, location), want: false},
		{name: "start", now: time.Date(2026, 7, 8, 6, 30, 0, 0, location), want: true},
		{name: "before end", now: time.Date(2026, 7, 8, 17, 14, 0, 0, location), want: true},
		{name: "end", now: time.Date(2026, 7, 8, 17, 15, 0, 0, location), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := scheduler.inBusinessWindow(test.now); got != test.want {
				t.Fatalf("inBusinessWindow(%s) = %t, want %t", test.now, got, test.want)
			}
		})
	}
}
