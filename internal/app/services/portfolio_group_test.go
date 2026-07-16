package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeafPortfolioTickersMatchesReferenceRefresherBehavior(t *testing.T) {
	nodes := []portfolioGroupNode{
		{PortfolioID: 1, PortfolioTicker: "PARENT", ChildPortfolioIDs: []string{"2"}},
		{PortfolioID: 2, PortfolioTicker: "710T"},
		{PortfolioID: 3, PortfolioTicker: "702T"},
		{PortfolioID: 4, PortfolioTicker: "702t"},
		{PortfolioID: 0, PortfolioTicker: "NO_ID"},
		{PortfolioID: 5, PortfolioTicker: ""},
	}
	got := leafPortfolioTickers(nodes)
	if len(got) != 2 || got[0] != "702T" || got[1] != "710T" {
		t.Fatalf("leaf portfolio tickers = %#v, want [702T 710T]", got)
	}
}

func TestPortfolioGroupPlanCachesMembersAndBatchesAt100(t *testing.T) {
	var memberCalls atomic.Int32
	var memberCount atomic.Int32
	memberCount.Store(705)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/portfolio/configuration/portfolio-group/v1/portfolioGroups/TCW_ALL/members" {
			t.Errorf("unexpected path %s", request.URL.Path)
			response.WriteHeader(http.StatusNotFound)
			return
		}
		memberCalls.Add(1)
		nodes := []portfolioGroupNode{{PortfolioID: 999999, PortfolioTicker: "PARENT", ChildPortfolioIDs: []string{"1"}}}
		for index := range int(memberCount.Load()) {
			nodes = append(nodes, portfolioGroupNode{PortfolioID: int64(index + 1), PortfolioTicker: fmt.Sprintf("P%04d", index+1)})
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(portfolioGroupMembersResponse{Nodes: nodes})
	}))
	defer server.Close()

	scheduler := newPortfolioGroupTestScheduler(server.URL, 60*time.Minute)
	now := time.Now()
	window := RefreshWindow{Start: now.Add(-5 * time.Minute), End: now, MaxCatchup: 7 * 24 * time.Hour, HasWork: true}
	first, err := scheduler.buildPortfolioRefreshPlan(context.Background(), "run-1", window)
	if err != nil {
		t.Fatal(err)
	}
	if first.MemberCount != 705 || len(first.RegularFilters) != 8 || len(first.BackfillFilters) != 8 {
		t.Fatalf("first plan members=%d regular=%d backfill=%d, want 705/8/8", first.MemberCount, len(first.RegularFilters), len(first.BackfillFilters))
	}
	if scheduler.portfolioMembership("TCW_ALL") != nil {
		t.Fatal("portfolio membership cache changed before the trade load and checkpoint committed")
	}
	for index, batch := range first.RegularFilters {
		if len(batch.References) == 0 || len(batch.References) > maxPortfolioReferencesPerTradeFilter {
			t.Fatalf("batch %d contains %d portfolios", index+1, len(batch.References))
		}
		for _, reference := range batch.References {
			if reference.PortfolioID == "" || reference.PortfolioTicker == "" {
				t.Fatalf("batch %d lost stable portfolio identity: %#v", index+1, reference)
			}
		}
	}
	scheduler.commitPortfolioMembership(first.Membership)

	second, err := scheduler.buildPortfolioRefreshPlan(context.Background(), "run-2", window)
	if err != nil {
		t.Fatal(err)
	}
	if memberCalls.Load() != 1 || second.Membership == nil || len(second.BackfillFilters) != 0 {
		t.Fatalf("cache hit calls=%d membership=%v backfill=%d", memberCalls.Load(), second.Membership != nil, len(second.BackfillFilters))
	}

	scheduler.portfolioCacheMu.Lock()
	scheduler.cfg.PortfolioGroupRefreshInterval = 30 * time.Minute
	scheduler.portfolioCache.FetchedAt = time.Now().Add(-31 * time.Minute)
	scheduler.portfolioCacheMu.Unlock()
	memberCount.Store(706)
	third, err := scheduler.buildPortfolioRefreshPlan(context.Background(), "run-3", window)
	if err != nil {
		t.Fatal(err)
	}
	if memberCalls.Load() != 2 || third.MemberCount != 706 || len(third.BackfillFilters) != 1 || len(third.BackfillFilters[0].Values) != 1 {
		t.Fatalf("refreshed plan calls=%d members=%d backfill=%#v", memberCalls.Load(), third.MemberCount, third.BackfillFilters)
	}
}

func TestPortfolioGroupPlanSeparatesRemovedMembersAndReconcilesSinceLastSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/portfolio/configuration/portfolio-group/v1/portfolioGroups/TCW_ALL/members" {
			t.Errorf("unexpected path %s", request.URL.Path)
			response.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(response).Encode(portfolioGroupMembersResponse{Nodes: []portfolioGroupNode{
			{PortfolioID: 2, PortfolioTicker: "ACTIVE"},
		}})
	}))
	defer server.Close()

	scheduler := newPortfolioGroupTestScheduler(server.URL, 30*time.Minute)
	now := time.Now()
	previousFetchedAt := now.Add(-31 * time.Minute)
	scheduler.commitPortfolioMembership(&portfolioMembershipCache{
		GroupTicker: "TCW_ALL",
		Members: []PortfolioReference{
			{PortfolioID: "1", PortfolioTicker: "REMOVED"},
			{PortfolioID: "2", PortfolioTicker: "ACTIVE"},
		},
		FetchedAt: previousFetchedAt,
	})
	window := RefreshWindow{Start: now.Add(-5 * time.Minute), End: now, MaxCatchup: 7 * 24 * time.Hour, HasWork: true}
	plan, err := scheduler.buildPortfolioRefreshPlan(context.Background(), "run-removed", window)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.RegularFilters) != 1 || len(plan.RegularFilters[0].References) != 1 || plan.RegularFilters[0].References[0].PortfolioID != "2" {
		t.Fatalf("active filters = %#v, want only portfolio 2", plan.RegularFilters)
	}
	if len(plan.RetiredFilters) != 1 || len(plan.RetiredFilters[0].References) != 1 || plan.RetiredFilters[0].References[0].PortfolioID != "1" {
		t.Fatalf("retired filters = %#v, want only portfolio 1", plan.RetiredFilters)
	}
	wantStart := previousFetchedAt.Add(-scheduler.cfg.Lookback)
	if !plan.RegularStart.Equal(previousFetchedAt) || !plan.RetiredStart.Equal(wantStart) {
		t.Fatalf("regular/retired starts = %s/%s, want %s/%s", plan.RegularStart, plan.RetiredStart, previousFetchedAt, wantStart)
	}
}

func TestPortfolioGroupBatchesProduceAtMost100ReferencesAndAcceptEmpty200(t *testing.T) {
	var sizesMu sync.Mutex
	var sizes []int
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/trading/trade-processing/trade/v2/trades:filter" {
			t.Errorf("unexpected path %s", request.URL.Path)
			response.WriteHeader(http.StatusNotFound)
			return
		}
		var body struct {
			Query struct {
				Criteria struct {
					Portfolio struct {
						PortfolioGroupTicker string `json:"portfolioGroupTicker"`
						PortfolioReferences  []any  `json:"portfolioReferences"`
					} `json:"portfolio"`
				} `json:"criteria"`
			} `json:"query"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode filter request: %v", err)
		}
		if body.Query.Criteria.Portfolio.PortfolioGroupTicker != "" {
			t.Errorf("oversized group ticker was sent directly")
		}
		sizesMu.Lock()
		sizes = append(sizes, len(body.Query.Criteria.Portfolio.PortfolioReferences))
		sizesMu.Unlock()
		current := inFlight.Add(1)
		for {
			maximum := maxInFlight.Load()
			if current <= maximum || maxInFlight.CompareAndSwap(maximum, current) {
				break
			}
		}
		defer inFlight.Add(-1)
		time.Sleep(10 * time.Millisecond)
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"status":{"code":200,"message":"Number of results processed successfully: 0/0"},"blockTrades":[]}`))
	}))
	defer server.Close()

	scheduler := newPortfolioGroupTestScheduler(server.URL, time.Hour)
	members := make([]string, 0, 705)
	for index := range 705 {
		members = append(members, fmt.Sprintf("P%04d", index+1))
	}
	start := time.Now().Add(-3 * time.Minute)
	summary, err := scheduler.fetchTradeWindowsForFilters(context.Background(), "run-1", start, start.Add(3*time.Minute), portfolioFilterBatches(members), nil)
	if err != nil {
		t.Fatal(err)
	}
	if summary.QueryWindowCount != 8 || summary.PageCount != 8 || len(sizes) != 8 {
		t.Fatalf("queries=%d pages=%d sizes=%v, want 8/8/eight batches", summary.QueryWindowCount, summary.PageCount, sizes)
	}
	if maxInFlight.Load() != maxConcurrentAladdinRequests {
		t.Fatalf("maximum concurrent filter requests = %d, want %d", maxInFlight.Load(), maxConcurrentAladdinRequests)
	}
	for index, size := range sizes {
		if size < 1 || size > 100 {
			t.Fatalf("request %d has %d portfolio references", index+1, size)
		}
	}
}

func TestExplicitPortfolioFiltersNeverResolvePortfolioGroup(t *testing.T) {
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		PortfolioFilter: PortfolioFilter{Type: PortfolioFilterReferences, Values: []string{"702T", "710T"}, Raw: "702T,710T"},
	}}
	plan, err := scheduler.buildPortfolioRefreshPlan(context.Background(), "run-1", RefreshWindow{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.MemberCount != 2 || len(plan.RegularFilters) != 1 || plan.Membership != nil {
		t.Fatalf("unexpected explicit portfolio plan: %#v", plan)
	}
}

func TestPortfolioGroupMembershipRestoresFromDurableCheckpoint(t *testing.T) {
	fetchedAt := time.Now().Add(-15 * time.Minute)
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		PortfolioFilter:               PortfolioFilter{Type: PortfolioFilterGroup, Values: []string{"TCW_ALL"}},
		PortfolioGroupRefreshInterval: time.Hour,
	}}
	scheduler.restorePortfolioMembership(CheckpointState{
		PortfolioGroupTicker:    "TCW_ALL",
		PortfolioGroupMembers:   []PortfolioReference{{PortfolioID: "1", PortfolioTicker: "702T"}, {PortfolioID: "2", PortfolioTicker: "710T"}},
		PortfolioGroupFetchedAt: fetchedAt,
	})

	got := scheduler.portfolioMembership("TCW_ALL")
	if got == nil || len(got.Members) != 2 || !got.FetchedAt.Equal(fetchedAt) {
		t.Fatalf("restored membership = %#v", got)
	}
}

func TestDecodePortfolioGroupMembersSupportsLegacyTickerSnapshots(t *testing.T) {
	members, err := decodePortfolioGroupMembers([]byte(`["702T","710T"]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[0].PortfolioTicker != "702T" || members[0].PortfolioID != "" {
		t.Fatalf("decoded legacy members = %#v", members)
	}
}

func TestPortfolioMembershipTreatsChangedStableIDAsRemoveAndAdd(t *testing.T) {
	previous := []PortfolioReference{{PortfolioID: "1", PortfolioTicker: "SAME"}}
	current := []PortfolioReference{{PortfolioID: "2", PortfolioTicker: "SAME"}}
	added, removed := portfolioMembershipChanges(previous, current)
	if len(added) != 1 || added[0].PortfolioID != "2" || len(removed) != 1 || removed[0].PortfolioID != "1" {
		t.Fatalf("added/removed = %#v/%#v", added, removed)
	}

	legacy := []PortfolioReference{{PortfolioTicker: "SAME"}}
	added, removed = portfolioMembershipChanges(legacy, current)
	if len(added) != 0 || len(removed) != 0 {
		t.Fatalf("legacy checkpoint migration should match by ticker, got %#v/%#v", added, removed)
	}
}

func newPortfolioGroupTestScheduler(baseURL string, refreshInterval time.Duration) *TradeScheduler {
	client := NewAladdinClient(AladdinClientConfig{
		BaseURL:            baseURL,
		TradePath:          "/trading/trade-processing/trade/v2",
		PortfolioGroupPath: "/portfolio/configuration/portfolio-group/v1/portfolioGroups",
		APIEnabled:         true,
	}, nil, NewSharedLimiter(RateLimitConfig{}))
	return &TradeScheduler{
		client: client,
		cfg: SchedulerConfig{
			PortfolioFilter:               PortfolioFilter{Type: PortfolioFilterGroup, Values: []string{"TCW_ALL"}, Raw: "group:TCW_ALL"},
			PortfolioGroupRefreshInterval: refreshInterval,
			PageSize:                      1000,
			DecodeWorkers:                 2,
			Lookback:                      5 * time.Minute,
			MaxCatchup:                    7 * 24 * time.Hour,
		},
	}
}
