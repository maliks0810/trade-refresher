package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"refresher/trade-refresher/internal/utils/log"
)

const maxPortfolioReferencesPerTradeFilter = 100

type portfolioGroupNode struct {
	ChildPortfolioIDs []string `json:"childPortfolioIds"`
	PortfolioID       int64    `json:"portfolioId"`
	PortfolioTicker   string   `json:"portfolioTicker"`
}

type portfolioGroupMembersResponse struct {
	Nodes []portfolioGroupNode `json:"nodes"`
}

type portfolioMembershipCache struct {
	GroupTicker string
	Members     []PortfolioReference
	FetchedAt   time.Time
}

type portfolioRefreshPlan struct {
	RegularFilters  []PortfolioFilter
	BackfillFilters []PortfolioFilter
	BackfillStart   time.Time
	BackfillEnd     time.Time
	RegularStart    time.Time
	RetiredStart    time.Time
	RetiredFilters  []PortfolioFilter
	Membership      *portfolioMembershipCache
	MemberCount     int
	FromCache       bool
}

func (s *TradeScheduler) buildPortfolioRefreshPlan(ctx context.Context, runID string, window RefreshWindow) (portfolioRefreshPlan, error) {
	return s.buildPortfolioRefreshPlanWithCachePolicy(ctx, runID, window, true)
}

func (s *TradeScheduler) buildPortfolioRefreshPlanWithCachePolicy(ctx context.Context, runID string, window RefreshWindow, allowCache bool) (portfolioRefreshPlan, error) {
	configured := s.cfg.PortfolioFilter
	if configured.Type != PortfolioFilterGroup {
		filters := portfolioFilterBatches(configured.Values)
		return portfolioRefreshPlan{RegularFilters: filters, MemberCount: len(configured.Values)}, nil
	}
	if len(configured.Values) != 1 {
		return portfolioRefreshPlan{}, fmt.Errorf("portfolio group filter requires exactly one ticker")
	}

	groupTicker := configured.Values[0]
	now := time.Now()
	refreshInterval := s.portfolioGroupRefreshInterval()
	previous := s.portfolioMembership(groupTicker)
	if allowCache && previous != nil {
		age := now.Sub(previous.FetchedAt)
		if age < 0 || age < refreshInterval {
			filters := portfolioMemberFilterBatches(previous.Members)
			log.Logger.Debug("portfolio_group.members.cache_hit",
				zap.String("refresh_run_id", runID),
				zap.String("portfolio_group", groupTicker),
				zap.Int("portfolio_member_count", len(previous.Members)),
				zap.Int("portfolio_batch_count", len(filters)),
				zap.Duration("cache_age", age),
				zap.Duration("refresh_interval", refreshInterval),
			)
			return portfolioRefreshPlan{RegularFilters: filters, Membership: previous, MemberCount: len(previous.Members), FromCache: true}, nil
		}
	}

	started := time.Now()
	members, nodeCount, err := s.client.portfolioGroupMembers(ctx, groupTicker, runID)
	if err != nil {
		return portfolioRefreshPlan{}, err
	}
	resolvedAt := time.Now()
	plan := portfolioRefreshPlan{
		RegularFilters: portfolioMemberFilterBatches(members),
		RegularStart:   window.Start,
		Membership: &portfolioMembershipCache{
			GroupTicker: groupTicker,
			Members:     append([]PortfolioReference(nil), members...),
			FetchedAt:   resolvedAt,
		},
		MemberCount: len(members),
	}

	added := members
	removed := []PortfolioReference(nil)
	backfillStart := window.Start.Add(-refreshInterval - s.cfg.Lookback)
	if previous != nil {
		added, removed = portfolioMembershipChanges(previous.Members, members)
		plan.RetiredFilters = portfolioMemberFilterBatches(removed)
		backfillStart = previous.FetchedAt.Add(-s.cfg.Lookback)
		plan.RetiredStart = window.Start
		retiredStart := previous.FetchedAt.Add(-s.cfg.Lookback)
		if retiredStart.Before(plan.RetiredStart) {
			plan.RetiredStart = s.clampMembershipBackfillStart(retiredStart, window.End)
		}
		reconciliationStart := previous.FetchedAt
		earliestSingleWindow := window.End.Add(-maxTradeFilterDuration)
		if reconciliationStart.Before(earliestSingleWindow) {
			reconciliationStart = earliestSingleWindow
		}
		if reconciliationStart.Before(plan.RegularStart) {
			plan.RegularStart = reconciliationStart
		}
	}
	backfillStart = s.clampMembershipBackfillStart(backfillStart, window.End)
	if len(added) > 0 && backfillStart.Before(window.Start) {
		plan.BackfillFilters = portfolioMemberFilterBatches(added)
		plan.BackfillStart = backfillStart
		plan.BackfillEnd = window.Start.Add(tradeFilterBoundaryOverlap)
		if plan.BackfillEnd.After(window.End) {
			plan.BackfillEnd = window.End
		}
	}

	log.Logger.Info("portfolio_group.members.refreshed",
		zap.String("refresh_run_id", runID),
		zap.String("portfolio_group", groupTicker),
		zap.Int("portfolio_node_count", nodeCount),
		zap.Int("portfolio_member_count", len(members)),
		zap.Int("portfolio_batch_count", len(plan.RegularFilters)),
		zap.Int("added_portfolio_count", len(added)),
		zap.Int("removed_portfolio_count", len(removed)),
		zap.Int("retired_portfolio_batch_count", len(plan.RetiredFilters)),
		zap.Int("backfill_portfolio_batch_count", len(plan.BackfillFilters)),
		zap.String("reconciliation_start_pt", pacificLogTimestamp(plan.RegularStart)),
		zap.String("retired_start_pt", pacificLogTimestamp(plan.RetiredStart)),
		zap.String("backfill_start_pt", pacificLogTimestamp(plan.BackfillStart)),
		zap.String("backfill_end_pt", pacificLogTimestamp(plan.BackfillEnd)),
		zap.Duration("refresh_interval", refreshInterval),
		zap.Duration("duration", time.Since(started)),
	)
	return plan, nil
}

func (s *TradeScheduler) portfolioGroupRefreshInterval() time.Duration {
	if s.cfg.PortfolioGroupRefreshInterval < time.Minute {
		return time.Hour
	}
	return s.cfg.PortfolioGroupRefreshInterval
}

func (s *TradeScheduler) portfolioMembership(groupTicker string) *portfolioMembershipCache {
	s.portfolioCacheMu.RLock()
	defer s.portfolioCacheMu.RUnlock()
	if s.portfolioCache == nil || !strings.EqualFold(s.portfolioCache.GroupTicker, groupTicker) {
		return nil
	}
	copy := *s.portfolioCache
	copy.Members = append([]PortfolioReference(nil), s.portfolioCache.Members...)
	return &copy
}

func (s *TradeScheduler) commitPortfolioMembership(update *portfolioMembershipCache) {
	if update == nil {
		return
	}
	copy := *update
	copy.Members = append([]PortfolioReference(nil), update.Members...)
	s.portfolioCacheMu.Lock()
	s.portfolioCache = &copy
	s.portfolioCacheMu.Unlock()
}

func (s *TradeScheduler) restorePortfolioMembership(checkpoint CheckpointState) {
	configured := s.cfg.PortfolioFilter
	if configured.Type != PortfolioFilterGroup || len(configured.Values) != 1 || len(checkpoint.PortfolioGroupMembers) == 0 || checkpoint.PortfolioGroupFetchedAt.IsZero() {
		return
	}
	if !strings.EqualFold(configured.Values[0], checkpoint.PortfolioGroupTicker) || s.portfolioMembership(configured.Values[0]) != nil {
		return
	}
	s.commitPortfolioMembership(&portfolioMembershipCache{
		GroupTicker: checkpoint.PortfolioGroupTicker,
		Members:     checkpoint.PortfolioGroupMembers,
		FetchedAt:   checkpoint.PortfolioGroupFetchedAt,
	})
}

func (s *TradeScheduler) clampMembershipBackfillStart(start, windowEnd time.Time) time.Time {
	earliest := windowEnd.Add(-s.maxCatchup())
	if start.Before(earliest) {
		return earliest
	}
	return start
}

func (p portfolioRefreshPlan) queryWindowCount(regularStart, regularEnd time.Time) int {
	if !p.RegularStart.IsZero() && p.RegularStart.Before(regularStart) {
		regularStart = p.RegularStart
	}
	windows := len(splitTradeFilterWindows(regularStart, regularEnd))
	count := windows * len(p.RegularFilters)
	retiredStart := regularStart
	if !p.RetiredStart.IsZero() {
		retiredStart = p.RetiredStart
	}
	count += len(splitTradeFilterWindows(retiredStart, regularEnd)) * len(p.RetiredFilters)
	if len(p.BackfillFilters) > 0 {
		count += len(splitTradeFilterWindows(p.BackfillStart, p.BackfillEnd)) * len(p.BackfillFilters)
	}
	return count
}

func (c *AladdinClient) portfolioGroupMembers(ctx context.Context, groupTicker, correlationID string) ([]PortfolioReference, int, error) {
	if c == nil {
		return nil, 0, fmt.Errorf("aladdin client is required to resolve portfolio group %q", groupTicker)
	}
	if strings.TrimSpace(c.cfg.PortfolioGroupPath) == "" {
		return nil, 0, fmt.Errorf("ALADDIN_PORTFOLIO_GROUP_PATH is required for portfolio group %q", groupTicker)
	}
	response, err := c.Do(
		ctx,
		portfolioGroupMembersOperation(c.cfg.PortfolioGroupPath),
		map[string]string{"portfolioGroup": groupTicker},
		nil,
		correlationID,
	)
	if err != nil {
		return nil, 0, err
	}
	if response == nil || response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		status := 0
		requestID := ""
		summary := "no response from BlackRock"
		if response != nil {
			status = response.StatusCode
			requestID = response.BlackRockRequestID
			if value := upstreamErrorSummary(response.Body); value != "" {
				summary = value
			}
		}
		return nil, 0, fmt.Errorf(
			"BlackRock portfolio group members request failed: group=%q status_code=%d blackrock_request_id=%q upstream_error=%s",
			groupTicker,
			status,
			requestID,
			summary,
		)
	}

	var payload portfolioGroupMembersResponse
	if err := json.Unmarshal(response.Body, &payload); err != nil {
		return nil, 0, fmt.Errorf("decode portfolio group %q members: %w", groupTicker, err)
	}
	members := leafPortfolioMembers(payload.Nodes)
	if len(members) == 0 {
		return nil, len(payload.Nodes), fmt.Errorf("portfolio group %q returned no leaf portfolios", groupTicker)
	}
	return members, len(payload.Nodes), nil
}

func leafPortfolioTickers(nodes []portfolioGroupNode) []string {
	members := leafPortfolioMembers(nodes)
	tickers := make([]string, 0, len(members))
	for _, member := range members {
		tickers = append(tickers, member.PortfolioTicker)
	}
	return tickers
}

func leafPortfolioMembers(nodes []portfolioGroupNode) []PortfolioReference {
	members := make([]PortfolioReference, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		ticker := strings.TrimSpace(node.PortfolioTicker)
		if node.PortfolioID <= 0 || len(node.ChildPortfolioIDs) != 0 || ticker == "" {
			continue
		}
		key := strings.ToUpper(ticker)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		members = append(members, PortfolioReference{
			PortfolioID:     fmt.Sprintf("%d", node.PortfolioID),
			PortfolioTicker: ticker,
		})
	}
	sort.Slice(members, func(i, j int) bool {
		return strings.ToUpper(members[i].PortfolioTicker) < strings.ToUpper(members[j].PortfolioTicker)
	})
	return members
}

func portfolioFilterBatches(members []string) []PortfolioFilter {
	if len(members) == 0 {
		return nil
	}
	batches := make([]PortfolioFilter, 0, (len(members)+maxPortfolioReferencesPerTradeFilter-1)/maxPortfolioReferencesPerTradeFilter)
	for start := 0; start < len(members); start += maxPortfolioReferencesPerTradeFilter {
		end := min(start+maxPortfolioReferencesPerTradeFilter, len(members))
		values := append([]string(nil), members[start:end]...)
		batches = append(batches, PortfolioFilter{Type: PortfolioFilterReferences, Values: values, Raw: strings.Join(values, ",")})
	}
	return batches
}

func portfolioMemberFilterBatches(members []PortfolioReference) []PortfolioFilter {
	if len(members) == 0 {
		return nil
	}
	batches := make([]PortfolioFilter, 0, (len(members)+maxPortfolioReferencesPerTradeFilter-1)/maxPortfolioReferencesPerTradeFilter)
	for start := 0; start < len(members); start += maxPortfolioReferencesPerTradeFilter {
		end := min(start+maxPortfolioReferencesPerTradeFilter, len(members))
		references := append([]PortfolioReference(nil), members[start:end]...)
		values := make([]string, 0, len(references))
		for _, reference := range references {
			values = append(values, reference.PortfolioTicker)
		}
		batches = append(batches, PortfolioFilter{Type: PortfolioFilterReferences, Values: values, References: references, Raw: strings.Join(values, ",")})
	}
	return batches
}

func portfolioMembershipChanges(previous, current []PortfolioReference) (added, removed []PortfolioReference) {
	previousMembers := newPortfolioMemberIndex(previous)
	currentMembers := newPortfolioMemberIndex(current)
	for _, member := range current {
		if !previousMembers.contains(member) {
			added = append(added, member)
		}
	}
	for _, member := range previous {
		if !currentMembers.contains(member) {
			removed = append(removed, member)
		}
	}
	return added, removed
}

type portfolioMemberIndex struct {
	ids           map[string]struct{}
	tickers       map[string]struct{}
	idlessTickers map[string]struct{}
}

func newPortfolioMemberIndex(values []PortfolioReference) portfolioMemberIndex {
	index := portfolioMemberIndex{
		ids:           make(map[string]struct{}, len(values)),
		tickers:       make(map[string]struct{}, len(values)),
		idlessTickers: make(map[string]struct{}),
	}
	for _, value := range values {
		id := strings.TrimSpace(value.PortfolioID)
		ticker := strings.ToUpper(strings.TrimSpace(value.PortfolioTicker))
		if id != "" {
			index.ids[id] = struct{}{}
		} else if ticker != "" {
			index.idlessTickers[ticker] = struct{}{}
		}
		if ticker != "" {
			index.tickers[ticker] = struct{}{}
		}
	}
	return index
}

func (i portfolioMemberIndex) contains(member PortfolioReference) bool {
	ticker := strings.ToUpper(strings.TrimSpace(member.PortfolioTicker))
	if id := strings.TrimSpace(member.PortfolioID); id != "" {
		if _, exists := i.ids[id]; exists {
			return true
		}
		_, legacyMatch := i.idlessTickers[ticker]
		return ticker != "" && legacyMatch
	}
	_, exists := i.tickers[ticker]
	return exists
}
