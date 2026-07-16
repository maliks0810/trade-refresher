package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"refresher/trade-refresher/internal/utils/log"
)

const tradeRefreshProcessName = "trade_refresh"
const maxTradeFilterPages = 1000
const maxTradeFilterDuration = time.Hour
const tradeFilterBoundaryOverlap = time.Second
const portfolioRecoveryChunkDuration = 4 * time.Hour

const (
	defaultBusinessStartMinute = 4 * 60
	defaultBusinessEndMinute   = 18 * 60
	defaultMaxCatchup          = 7 * 24 * time.Hour
)

type SchedulerConfig struct {
	Enabled                       bool
	Interval                      time.Duration
	PortfolioFilter               PortfolioFilter
	PortfolioGroupRefreshInterval time.Duration
	PageSize                      int
	Lookback                      time.Duration
	SafetyDelay                   time.Duration
	BusinessStartMinutePT         int
	BusinessEndMinutePT           int
	MaxCatchup                    time.Duration
	LockTTL                       time.Duration
	DecodeWorkers                 int
	GemReportAfterFailures        int
}

type RefreshWindow struct {
	Start      time.Time
	End        time.Time
	Lag        time.Duration
	MaxCatchup time.Duration
	WasClipped bool
	FirstRun   bool
	HasWork    bool
}

type TradeLoadSummary struct {
	QueryWindowCount    int
	PortfolioBatchCount int
	PageCount           int
	BlockCount          int
	TradeCount          int
	RowCount            int
	Duplicates          int
	MergeBatchCount     int
	MergePayloadBytes   int
	NewRecoveries       []PortfolioRecovery
}

type tradeFilterWindow struct {
	Start time.Time
	End   time.Time
}

type blackRockFilterError struct {
	StatusCode     int
	RequestID      string
	Attempts       int
	Window         tradeFilterWindow
	PortfolioCount int
	Summary        string
	Cause          error
}

func (e *blackRockFilterError) Error() string {
	return fmt.Sprintf(
		"BlackRock /trades:filter failed: status_code=%d blackrock_request_id=%q attempts=%d portfolio_count=%d modify_time_start_pt=%s modify_time_end_pt=%s upstream_error=%s",
		e.StatusCode,
		e.RequestID,
		e.Attempts,
		e.PortfolioCount,
		pacificLogTimestamp(e.Window.Start),
		pacificLogTimestamp(e.Window.End),
		e.Summary,
	)
}

func (e *blackRockFilterError) Unwrap() error {
	return e.Cause
}

type snowflakePageMergeError struct {
	page int
	err  error
}

func (e *snowflakePageMergeError) Error() string {
	return fmt.Sprintf("Snowflake merge failed for trade page %d: %v", e.page, e.err)
}

func (e *snowflakePageMergeError) Unwrap() error {
	return e.err
}

type TradeScheduler struct {
	cfg                 SchedulerConfig
	client              *AladdinClient
	store               *SnowflakeStore
	gem                 *GemClient
	ownerID             string
	consecutiveFailures int
	portfolioCacheMu    sync.RWMutex
	portfolioCache      *portfolioMembershipCache
}

func NewTradeScheduler(cfg SchedulerConfig, client *AladdinClient, store *SnowflakeStore, gem *GemClient) *TradeScheduler {
	owner := os.Getenv("HOSTNAME")
	if owner == "" {
		owner = "trade-refresher-local"
	}
	return &TradeScheduler{cfg: cfg, client: client, store: store, gem: gem, ownerID: owner}
}

func (s *TradeScheduler) Start(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if s == nil || !s.cfg.Enabled || s.client == nil || s.store == nil {
		log.Logger.Info("refresh.scheduler.disabled")
		close(done)
		return done
	}
	interval := s.cfg.Interval
	if interval < time.Minute {
		interval = 3 * time.Minute
	}
	log.Logger.Info("refresh.scheduler.started",
		zap.Duration("refresh_interval", interval),
		zap.String("timezone", pacificLocation().String()),
		zap.String("business_start_time_pt", formatMinuteOfDay(s.businessStartMinutePT())),
		zap.String("business_end_time_pt", formatMinuteOfDay(s.businessEndMinutePT())),
		zap.String("max_catchup", catchupDurationLabel(s.maxCatchup())),
	)
	go func() {
		defer close(done)
		for {
			runStarted := time.Now()
			s.runOnce(ctx)
			delay := time.Until(runStarted.Add(interval))
			if delay < 0 {
				delay = 0
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}
	}()
	return done
}

func (s *TradeScheduler) runOnce(ctx context.Context) {
	now := time.Now()
	if !s.inBusinessWindow(now) {
		return
	}
	runStarted := time.Now()

	runID := uuid.NewString()
	token := uuid.NewString()
	ok, err := s.store.AcquireLease(ctx, tradeRefreshProcessName, s.ownerID, token, s.cfg.LockTTL)
	if err != nil {
		s.reportFailure(ctx, runID, DatabaseConnectionException, "Snowflake lease acquisition failed", err, "phase=acquire_lease")
		return
	}
	if !ok {
		log.Logger.Info("snowflake.lock.skipped", zap.String("owner", s.ownerID))
		return
	}
	leaseCtx, stopHeartbeat := context.WithCancelCause(ctx)
	leaseReleased := false
	defer func() {
		if leaseReleased {
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.store.ReleaseLease(releaseCtx, tradeRefreshProcessName, s.ownerID, token); err != nil {
			log.Logger.Warn("snowflake.lock.release_failed", zap.Error(err))
		}
	}()
	defer stopHeartbeat(nil)
	s.startLeaseHeartbeat(leaseCtx, runID, token, stopHeartbeat)

	checkpoint, err := s.store.LoadCheckpoint(leaseCtx, tradeRefreshProcessName)
	if err != nil {
		s.reportFailure(ctx, runID, DatabaseConnectionException, "Snowflake checkpoint load failed", err, "phase=load_checkpoint")
		return
	}
	portfolioFilterKey := s.cfg.PortfolioFilter.checkpointKey()
	if checkpoint.PortfolioFilterKey != portfolioFilterKey {
		log.Logger.Info("refresh.portfolio_filter.changed",
			zap.String("previous_portfolio_filter_key", checkpoint.PortfolioFilterKey),
			zap.String("portfolio_filter_key", portfolioFilterKey),
			zap.String("max_catchup", catchupDurationLabel(s.maxCatchup())),
		)
		checkpoint.LastSuccessfulEnd = time.Time{}
	}
	s.restorePortfolioMembership(checkpoint)
	window := s.nextRefreshWindow(now, checkpoint)
	if !window.HasWork {
		s.consecutiveFailures = 0
		return
	}
	if window.WasClipped {
		log.Logger.Warn("refresh.catchup_window.clipped",
			zap.String("refresh_run_id", runID),
			zap.String("checkpoint_end_pt", pacificLogTimestamp(checkpoint.LastSuccessfulEnd)),
			zap.String("effective_window_start_pt", pacificLogTimestamp(window.Start)),
			zap.String("max_catchup", catchupDurationLabel(window.MaxCatchup)),
		)
	}
	portfolioPlan, err := s.buildPortfolioRefreshPlan(leaseCtx, runID, window)
	if err != nil {
		s.reportFailure(ctx, runID, ApiException, "Aladdin portfolio group resolution failed", err, fmt.Sprintf("phase=resolve_portfolios;portfolio_filter=%s", s.cfg.PortfolioFilter.String()))
		return
	}
	log.Logger.Info("refresh.run.started",
		zap.String("refresh_run_id", runID),
		zap.String("portfolio_filter_type", string(s.cfg.PortfolioFilter.Type)),
		zap.String("portfolio_filter_value", s.cfg.PortfolioFilter.String()),
		zap.String("window_start_pt", pacificLogTimestamp(window.Start)),
		zap.String("window_end_pt", pacificLogTimestamp(window.End)),
		zap.Duration("catchup_lag", window.Lag),
		zap.String("max_catchup", catchupDurationLabel(window.MaxCatchup)),
		zap.Bool("catchup_window_clipped", window.WasClipped),
		zap.Bool("first_refresh_window", window.FirstRun),
		zap.Int("portfolio_member_count", portfolioPlan.MemberCount),
		zap.Int("portfolio_batch_count", len(portfolioPlan.RegularFilters)),
		zap.Int("backfill_portfolio_batch_count", len(portfolioPlan.BackfillFilters)),
		zap.Int("retired_portfolio_batch_count", len(portfolioPlan.RetiredFilters)),
		zap.Int("api_query_window_count", portfolioPlan.queryWindowCount(window.Start, window.End)),
	)

	load, err := s.loadTradeRows(leaseCtx, runID, window.Start, window.End, portfolioPlan)
	if err != nil && portfolioPlan.FromCache && isPermanentPortfolioFilterError(err) {
		fields := []zap.Field{
			zap.String("refresh_run_id", runID),
			zap.String("portfolio_group", s.cfg.PortfolioFilter.String()),
			zap.Error(err),
		}
		fields = append(fields, refreshFailureLogFields(err)...)
		log.Logger.Warn("portfolio_group.members.revalidation_requested", fields...)
		refreshedPlan, refreshErr := s.buildPortfolioRefreshPlanWithCachePolicy(leaseCtx, runID, window, false)
		if refreshErr != nil {
			err = fmt.Errorf("refresh portfolio membership after filter validation failure: %w", refreshErr)
		} else {
			portfolioPlan = refreshedPlan
			load, err = s.loadTradeRows(leaseCtx, runID, window.Start, window.End, portfolioPlan)
		}
	}
	if err != nil {
		exceptionName := ApiException
		message := "Aladdin trade refresh failed"
		phase := "fetch_trades"
		if cause := context.Cause(leaseCtx); cause != nil && ctx.Err() == nil {
			exceptionName = DatabaseConnectionException
			message = "Snowflake refresh lease heartbeat failed"
			phase = "extend_lease"
			err = cause
		}
		var mergeErr *snowflakePageMergeError
		if phase == "fetch_trades" && errors.As(err, &mergeErr) {
			exceptionName = DatabaseConnectionException
			message = "Snowflake trade page merge failed"
			phase = "merge_trade_page"
		}
		s.reportFailure(ctx, runID, exceptionName, message, err, fmt.Sprintf("phase=%s;window_start_pt=%s;window_end_pt=%s", phase, pacificLogTimestamp(window.Start), pacificLogTimestamp(window.End)))
		return
	}
	s.retryDuePortfolioRecoveries(leaseCtx, runID)
	run := RefreshRun{
		RunID:              runID,
		ProcessName:        tradeRefreshProcessName,
		LockOwner:          s.ownerID,
		LockToken:          token,
		WindowStart:        window.Start,
		WindowEnd:          window.End,
		PageCount:          load.PageCount,
		RowsWritten:        load.RowCount,
		PortfolioFilterKey: portfolioFilterKey,
		NewRecoveries:      load.NewRecoveries,
	}
	if portfolioPlan.Membership != nil {
		run.PortfolioGroupTicker = portfolioPlan.Membership.GroupTicker
		run.PortfolioGroupMembers = portfolioPlan.Membership.Members
		run.PortfolioGroupFetchedAt = portfolioPlan.Membership.FetchedAt
	}
	if err := s.store.CommitCheckpoint(leaseCtx, run); err != nil {
		s.reportFailure(ctx, runID, DatabaseConnectionException, "Snowflake trade checkpoint commit failed", err, fmt.Sprintf("phase=commit_checkpoint;page_count=%d;rows_loaded=%d", load.PageCount, load.RowCount))
		return
	}
	s.commitPortfolioMembership(portfolioPlan.Membership)
	leaseReleased = true
	s.consecutiveFailures = 0

	log.Logger.Info("refresh.run.completed",
		zap.String("refresh_run_id", runID),
		zap.Duration("duration", time.Since(runStarted)),
		zap.Int("api_query_window_count", load.QueryWindowCount),
		zap.Int("portfolio_batch_count", load.PortfolioBatchCount),
		zap.Int("api_page_count", load.PageCount),
		zap.Int("loaded_block_count", load.BlockCount),
		zap.Int("loaded_trade_count", load.TradeCount),
		zap.Int("loaded_row_count", load.RowCount),
		zap.Int("duplicate_rows_removed", load.Duplicates),
		zap.Int("snowflake_merge_batch_count", load.MergeBatchCount),
		zap.Int("snowflake_merge_payload_bytes", load.MergePayloadBytes),
		zap.Int("new_portfolio_recovery_count", len(load.NewRecoveries)),
		zap.String("checkpoint_committed_pt", pacificLogTimestamp(window.End)),
	)
}

func (s *TradeScheduler) nextRefreshWindow(now time.Time, checkpoint CheckpointState) RefreshWindow {
	fullEnd := now.Add(-s.cfg.SafetyDelay)
	maxCatchup := s.maxCatchup()
	earliestStart := fullEnd.Add(-maxCatchup)

	firstRun := checkpoint.LastSuccessfulEnd.IsZero()
	windowStart := earliestStart
	if !firstRun {
		windowStart = checkpoint.LastSuccessfulEnd.Add(-s.cfg.Lookback)
	}
	clipped := false
	if windowStart.Before(earliestStart) {
		windowStart = earliestStart
		clipped = true
	}
	lag := fullEnd.Sub(windowStart)

	return RefreshWindow{
		Start:      windowStart,
		End:        fullEnd,
		Lag:        lag,
		MaxCatchup: maxCatchup,
		WasClipped: clipped,
		FirstRun:   firstRun,
		HasWork:    fullEnd.After(windowStart),
	}
}

func (s *TradeScheduler) maxCatchup() time.Duration {
	if s.cfg.MaxCatchup <= 0 {
		return defaultMaxCatchup
	}
	return s.cfg.MaxCatchup
}

func catchupDurationLabel(duration time.Duration) string {
	if duration%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dD", duration/(24*time.Hour))
	}
	if duration%time.Hour == 0 {
		return fmt.Sprintf("%dH", duration/time.Hour)
	}
	return duration.String()
}

func (s *TradeScheduler) startLeaseHeartbeat(ctx context.Context, runID, token string, cancel context.CancelCauseFunc) {
	interval := s.cfg.LockTTL / 3
	if interval <= 0 || interval > time.Minute {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.store.ExtendLease(ctx, tradeRefreshProcessName, s.ownerID, token, s.cfg.LockTTL); err != nil {
					log.Logger.Error("snowflake.lock.extend_failed",
						zap.String("refresh_run_id", runID),
						zap.String("lock_owner", s.ownerID),
						zap.Error(err),
					)
					cancel(err)
					return
				}
			}
		}
	}()
}

func (s *TradeScheduler) loadTradeRows(ctx context.Context, runID string, windowStart, windowEnd time.Time, plan portfolioRefreshPlan) (TradeLoadSummary, error) {
	consume := func(pageNumber int, page TradeDataset) (MergePageResult, error) {
		if page.TotalRows() == 0 {
			return MergePageResult{}, nil
		}
		result, err := s.store.MergePage(ctx, runID, pageNumber, page)
		if err != nil {
			return MergePageResult{}, &snowflakePageMergeError{page: pageNumber, err: err}
		}
		return result, nil
	}

	summary := TradeLoadSummary{}
	if len(plan.BackfillFilters) > 0 && plan.BackfillEnd.After(plan.BackfillStart) {
		backfill, err := s.fetchTradeWindowsForFiltersAtOffset(ctx, runID, plan.BackfillStart, plan.BackfillEnd, summary.PageCount, plan.BackfillFilters, consume)
		summary.add(backfill)
		if err != nil {
			return summary, fmt.Errorf("new portfolio membership backfill failed: %w", err)
		}
	}
	regularStart := windowStart
	if !plan.RegularStart.IsZero() && plan.RegularStart.Before(regularStart) {
		regularStart = plan.RegularStart
	}
	regular, err := s.fetchTradeWindowsForFiltersAtOffset(ctx, runID, regularStart, windowEnd, summary.PageCount, plan.RegularFilters, consume)
	summary.add(regular)
	if err != nil {
		return summary, err
	}
	retiredStart := windowStart
	if !plan.RetiredStart.IsZero() && plan.RetiredStart.Before(retiredStart) {
		retiredStart = plan.RetiredStart
	}
	retired, err := s.drainRetiredPortfolioFilters(ctx, runID, retiredStart, windowEnd, summary.PageCount, plan.RetiredFilters, consume)
	summary.add(retired)
	return summary, err
}

func (s *TradeScheduler) drainRetiredPortfolioFilters(ctx context.Context, runID string, start, end time.Time, pageOffset int, filters []PortfolioFilter, consume func(int, TradeDataset) (MergePageResult, error)) (TradeLoadSummary, error) {
	summary := TradeLoadSummary{}
	for _, filter := range filters {
		result, err := s.drainRetiredPortfolioFilter(ctx, runID, start, end, pageOffset+summary.PageCount, filter, consume)
		summary.add(result)
		if err != nil {
			return summary, err
		}
	}
	return summary, nil
}

func (s *TradeScheduler) drainRetiredPortfolioFilter(ctx context.Context, runID string, start, end time.Time, pageOffset int, filter PortfolioFilter, consume func(int, TradeDataset) (MergePageResult, error)) (TradeLoadSummary, error) {
	summary, err := s.fetchTradeWindowsForFiltersAtOffset(ctx, runID, start, end, pageOffset, []PortfolioFilter{filter}, consume)
	if err == nil {
		return summary, nil
	}
	if isPermanentPortfolioFilterError(err) && filter.referenceCount() > 1 {
		left, right := filter.split()
		leftSummary, leftErr := s.drainRetiredPortfolioFilter(ctx, runID, start, end, pageOffset+summary.PageCount, left, consume)
		summary.add(leftSummary)
		if leftErr != nil {
			return summary, leftErr
		}
		rightSummary, rightErr := s.drainRetiredPortfolioFilter(ctx, runID, start, end, pageOffset+summary.PageCount, right, consume)
		summary.add(rightSummary)
		return summary, rightErr
	}
	references := filterPortfolioReferences(filter)
	for _, reference := range references {
		recovery := newPortfolioRecovery(reference, start, end, s.portfolioGroupRefreshInterval(), err)
		summary.NewRecoveries = append(summary.NewRecoveries, recovery)
		fields := []zap.Field{
			zap.String("refresh_run_id", runID),
			zap.String("recovery_id", recovery.RecoveryID),
			zap.String("portfolio_id", reference.PortfolioID),
			zap.String("portfolio_ticker", reference.PortfolioTicker),
			zap.String("window_start_pt", pacificLogTimestamp(start)),
			zap.String("window_end_pt", pacificLogTimestamp(end)),
			zap.String("next_attempt_pt", pacificLogTimestamp(recovery.NextAttempt)),
			zap.Error(err),
		}
		fields = append(fields, refreshFailureLogFields(err)...)
		log.Logger.Warn("portfolio_recovery.queued", fields...)
	}
	return summary, nil
}

func filterPortfolioReferences(filter PortfolioFilter) []PortfolioReference {
	if len(filter.References) > 0 {
		return append([]PortfolioReference(nil), filter.References...)
	}
	references := make([]PortfolioReference, 0, len(filter.Values))
	for _, ticker := range filter.Values {
		references = append(references, PortfolioReference{PortfolioTicker: ticker})
	}
	return references
}

func newPortfolioRecovery(reference PortfolioReference, start, end time.Time, retryInterval time.Duration, err error) PortfolioRecovery {
	identity := reference.PortfolioID
	if identity == "" {
		identity = strings.ToUpper(reference.PortfolioTicker)
	}
	return PortfolioRecovery{
		RecoveryID:  "trade_refresh_recovery:" + hashParts(identity, start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))[:24],
		Reference:   reference,
		Start:       start,
		End:         end,
		NextAttempt: time.Now().Add(retryInterval),
		Attempts:    1,
		LastError:   err.Error(),
	}
}

func (s *TradeScheduler) fetchTradeWindows(ctx context.Context, runID string, windowStart, windowEnd time.Time, consume func(int, TradeDataset) (MergePageResult, error)) (TradeLoadSummary, error) {
	return s.fetchTradeWindowsForFilters(ctx, runID, windowStart, windowEnd, []PortfolioFilter{s.cfg.PortfolioFilter}, consume)
}

func (s *TradeScheduler) fetchTradeWindowsForFilters(ctx context.Context, runID string, windowStart, windowEnd time.Time, filters []PortfolioFilter, consume func(int, TradeDataset) (MergePageResult, error)) (TradeLoadSummary, error) {
	return s.fetchTradeWindowsForFiltersAtOffset(ctx, runID, windowStart, windowEnd, 0, filters, consume)
}

func (s *TradeScheduler) fetchTradeWindowsForFiltersAtOffset(ctx context.Context, runID string, windowStart, windowEnd time.Time, pageOffset int, filters []PortfolioFilter, consume func(int, TradeDataset) (MergePageResult, error)) (TradeLoadSummary, error) {
	windows := splitTradeFilterWindows(windowStart, windowEnd)
	summary := TradeLoadSummary{QueryWindowCount: len(windows) * len(filters), PortfolioBatchCount: len(filters)}
	var pageCounter atomic.Int64
	pageCounter.Store(int64(pageOffset))
	var consumeMu sync.Mutex
	serializedConsume := consume
	if consume != nil {
		serializedConsume = func(pageNumber int, page TradeDataset) (MergePageResult, error) {
			consumeMu.Lock()
			defer consumeMu.Unlock()
			return consume(pageNumber, page)
		}
	}
	for index, window := range windows {
		results := make([]TradeLoadSummary, len(filters))
		batchErrors := make([]error, len(filters))
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(maxConcurrentAladdinRequests)
		for batchIndex, filter := range filters {
			batchIndex := batchIndex
			filter := filter
			group.Go(func() error {
				result, err := s.fetchTradePagesForFilter(groupCtx, runID, window.Start, window.End, filter, func() int {
					return int(pageCounter.Add(1))
				}, serializedConsume)
				results[batchIndex] = result
				batchErrors[batchIndex] = err
				return err
			})
		}
		groupErr := group.Wait()
		for _, result := range results {
			summary.addData(result)
		}
		batchIndex, batchErr := mostUsefulBatchError(batchErrors, groupErr)
		if batchErr != nil {
			failedFilter := filters[batchIndex]
			return summary, fmt.Errorf(
				"trade filter window %d/%d portfolio batch %d/%d failed (portfolio_count=%d batch_hash=%s start_pt=%s end_pt=%s): %w",
				index+1,
				len(windows),
				batchIndex+1,
				len(filters),
				failedFilter.referenceCount(),
				hashParts(failedFilter.String())[:12],
				pacificLogTimestamp(window.Start),
				pacificLogTimestamp(window.End),
				batchErr,
			)
		}
	}
	return summary, nil
}

func mostUsefulBatchError(batchErrors []error, groupErr error) (int, error) {
	selectedIndex := 0
	selected := groupErr
	for index, err := range batchErrors {
		if err == nil {
			continue
		}
		if selected == nil {
			selectedIndex, selected = index, err
		}
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return index, err
		}
	}
	return selectedIndex, selected
}

func (s *TradeScheduler) fetchTradePages(ctx context.Context, runID string, windowStart, windowEnd time.Time, consume func(int, TradeDataset) (MergePageResult, error)) (TradeLoadSummary, error) {
	return s.fetchTradePagesAtOffset(ctx, runID, windowStart, windowEnd, 0, consume)
}

func (s *TradeScheduler) fetchTradePagesAtOffset(ctx context.Context, runID string, windowStart, windowEnd time.Time, pageOffset int, consume func(int, TradeDataset) (MergePageResult, error)) (TradeLoadSummary, error) {
	return s.fetchTradePagesAtOffsetForFilter(ctx, runID, windowStart, windowEnd, pageOffset, s.cfg.PortfolioFilter, consume)
}

func (s *TradeScheduler) fetchTradePagesAtOffsetForFilter(ctx context.Context, runID string, windowStart, windowEnd time.Time, pageOffset int, filter PortfolioFilter, consume func(int, TradeDataset) (MergePageResult, error)) (TradeLoadSummary, error) {
	pageNumber := pageOffset
	return s.fetchTradePagesForFilter(ctx, runID, windowStart, windowEnd, filter, func() int {
		pageNumber++
		return pageNumber
	}, consume)
}

func (s *TradeScheduler) fetchTradePagesForFilter(ctx context.Context, runID string, windowStart, windowEnd time.Time, filter PortfolioFilter, nextPageNumber func() int, consume func(int, TradeDataset) (MergePageResult, error)) (TradeLoadSummary, error) {
	pageToken := ""
	summary := TradeLoadSummary{}
	seenTokens := map[string]struct{}{}
	requestCtx := ctx
	if s.cfg.PortfolioFilter.Type == PortfolioFilterGroup && len(s.cfg.PortfolioFilter.Values) == 1 {
		requestCtx = withPortfolioGroupLogContext(ctx, s.cfg.PortfolioFilter.Values[0])
	}
	for {
		if summary.PageCount >= maxTradeFilterPages {
			return summary, fmt.Errorf("trade filter window exceeded the %d-page safety limit", maxTradeFilterPages)
		}
		body, err := s.filterRequestBodyFor(windowStart, windowEnd, pageToken, filter)
		if err != nil {
			return summary, err
		}
		response, err := s.client.Do(requestCtx, OperationFilterTrades, nil, body, runID)
		if err != nil {
			return summary, err
		}
		if response == nil || response.StatusCode < 200 || response.StatusCode > 299 {
			status := 0
			requestID := ""
			errorSummary := "no response from BlackRock"
			if response != nil {
				status = response.StatusCode
				requestID = response.BlackRockRequestID
				errorSummary = upstreamErrorSummary(response.Body)
				if errorSummary == "" {
					errorSummary = "empty upstream error body"
				}
			}
			return summary, &blackRockFilterError{
				StatusCode:     status,
				RequestID:      requestID,
				Attempts:       responseAttempts(response),
				Window:         tradeFilterWindow{Start: windowStart, End: windowEnd},
				PortfolioCount: filter.referenceCount(),
				Summary:        errorSummary,
			}
		}
		summary.PageCount++
		pageNumber := nextPageNumber()
		meta := TradePageMetadata{
			PortfolioFilterType:  s.cfg.PortfolioFilter.Type,
			PortfolioFilterValue: s.cfg.PortfolioFilter.String(),
			RefreshRunID:         runID,
			WatermarkStart:       windowStart,
			WatermarkEnd:         windowEnd,
			PageNumber:           pageNumber,
			IngestedAt:           time.Now(),
			DecodeWorkers:        s.cfg.DecodeWorkers,
		}
		rows, nextPageToken, err := BuildTradeDatasetFromFilterResponse(response.Body, meta)
		response.Body = nil
		if err != nil {
			return summary, &blackRockFilterError{
				StatusCode:     response.StatusCode,
				RequestID:      response.BlackRockRequestID,
				Attempts:       response.Attempts,
				Window:         tradeFilterWindow{Start: windowStart, End: windowEnd},
				PortfolioCount: filter.referenceCount(),
				Summary:        err.Error(),
				Cause:          err,
			}
		}
		rows.Deduplicate()
		summary.BlockCount += rows.BlockCount
		summary.TradeCount += rows.TradeCount
		summary.RowCount += rows.TotalRows()
		summary.Duplicates += rows.DuplicatesRemoved
		if consume != nil {
			merge, err := consume(pageNumber, rows)
			if err != nil {
				return summary, err
			}
			summary.MergeBatchCount += merge.BatchCount
			summary.MergePayloadBytes += merge.PayloadBytes
		}
		if nextPageToken == "" {
			break
		}
		if _, exists := seenTokens[nextPageToken]; exists {
			return summary, fmt.Errorf("repeated nextPageToken detected on page %d", pageNumber)
		}
		seenTokens[nextPageToken] = struct{}{}
		pageToken = nextPageToken
	}
	return summary, nil
}

func splitTradeFilterWindows(start, end time.Time) []tradeFilterWindow {
	if !end.After(start) {
		return nil
	}
	windows := make([]tradeFilterWindow, 0, int(end.Sub(start)/maxTradeFilterDuration)+1)
	for cursor := start; cursor.Before(end); {
		next := cursor.Add(maxTradeFilterDuration)
		if next.After(end) {
			next = end
		}
		windows = append(windows, tradeFilterWindow{Start: cursor, End: next})
		if next.Equal(end) {
			break
		}
		cursor = next
	}
	return windows
}

func (s *TradeLoadSummary) add(other TradeLoadSummary) {
	s.QueryWindowCount += other.QueryWindowCount
	s.PortfolioBatchCount += other.PortfolioBatchCount
	s.addData(other)
}

func (s *TradeLoadSummary) addData(other TradeLoadSummary) {
	s.PageCount += other.PageCount
	s.BlockCount += other.BlockCount
	s.TradeCount += other.TradeCount
	s.RowCount += other.RowCount
	s.Duplicates += other.Duplicates
	s.MergeBatchCount += other.MergeBatchCount
	s.MergePayloadBytes += other.MergePayloadBytes
	s.NewRecoveries = append(s.NewRecoveries, other.NewRecoveries...)
}

func responseAttempts(response *AladdinResponse) int {
	if response == nil {
		return 0
	}
	return response.Attempts
}

func isPermanentPortfolioFilterError(err error) bool {
	var upstream *blackRockFilterError
	if errors.As(err, &upstream) && (upstream.StatusCode == http.StatusBadRequest || upstream.StatusCode == http.StatusNotFound) {
		return true
	}
	var status *tradeFilterStatusError
	if !errors.As(err, &status) {
		return false
	}
	switch status.Code {
	case 3, 5, 9, 11:
		return true
	default:
		return status.HasCounts || status.DetailsCount > 0
	}
}

func (s *TradeScheduler) retryDuePortfolioRecoveries(ctx context.Context, runID string) {
	recoveries, err := s.store.LoadDuePortfolioRecoveries(ctx)
	if err != nil {
		log.Logger.Error("portfolio_recovery.load_failed",
			zap.String("refresh_run_id", runID),
			zap.Error(err),
		)
		return
	}
	for _, recovery := range recoveries {
		chunkEnd := recovery.Start.Add(portfolioRecoveryChunkDuration)
		if chunkEnd.After(recovery.End) {
			chunkEnd = recovery.End
		}
		filters := portfolioMemberFilterBatches([]PortfolioReference{recovery.Reference})
		consume := func(pageNumber int, page TradeDataset) (MergePageResult, error) {
			if page.TotalRows() == 0 {
				return MergePageResult{}, nil
			}
			result, mergeErr := s.store.MergePage(ctx, runID, pageNumber, page)
			if mergeErr != nil {
				return MergePageResult{}, &snowflakePageMergeError{page: pageNumber, err: mergeErr}
			}
			return result, nil
		}
		log.Logger.Info("portfolio_recovery.started",
			zap.String("refresh_run_id", runID),
			zap.String("recovery_id", recovery.RecoveryID),
			zap.String("portfolio_id", recovery.Reference.PortfolioID),
			zap.String("portfolio_ticker", recovery.Reference.PortfolioTicker),
			zap.Int("attempt", recovery.Attempts+1),
			zap.String("window_start_pt", pacificLogTimestamp(recovery.Start)),
			zap.String("window_end_pt", pacificLogTimestamp(chunkEnd)),
		)
		_, recoveryErr := s.fetchTradeWindowsForFilters(ctx, runID, recovery.Start, chunkEnd, filters, consume)
		if recoveryErr != nil {
			recovery.Attempts++
			recovery.LastError = recoveryErr.Error()
			recovery.NextAttempt = time.Now().Add(s.portfolioGroupRefreshInterval())
			if updateErr := s.store.ReschedulePortfolioRecovery(ctx, recovery); updateErr != nil {
				log.Logger.Error("portfolio_recovery.reschedule_failed",
					zap.String("refresh_run_id", runID),
					zap.String("recovery_id", recovery.RecoveryID),
					zap.Error(updateErr),
				)
			}
			fields := []zap.Field{
				zap.String("refresh_run_id", runID),
				zap.String("recovery_id", recovery.RecoveryID),
				zap.String("portfolio_id", recovery.Reference.PortfolioID),
				zap.String("portfolio_ticker", recovery.Reference.PortfolioTicker),
				zap.Int("attempt", recovery.Attempts),
				zap.String("next_attempt_pt", pacificLogTimestamp(recovery.NextAttempt)),
				zap.Error(recoveryErr),
			}
			fields = append(fields, refreshFailureLogFields(recoveryErr)...)
			log.Logger.Error("portfolio_recovery.failed", fields...)
			s.reportPortfolioRecoveryFailure(ctx, runID, recovery, recoveryErr)
			continue
		}
		if chunkEnd.Before(recovery.End) {
			recovery.Start = chunkEnd.Add(-tradeFilterBoundaryOverlap)
			recovery.NextAttempt = time.Now()
			recovery.LastError = ""
			if err := s.store.ReschedulePortfolioRecovery(ctx, recovery); err != nil {
				log.Logger.Error("portfolio_recovery.advance_failed", zap.String("refresh_run_id", runID), zap.String("recovery_id", recovery.RecoveryID), zap.Error(err))
			}
			continue
		}
		if err := s.store.CompletePortfolioRecovery(ctx, recovery.RecoveryID); err != nil {
			log.Logger.Error("portfolio_recovery.complete_failed", zap.String("refresh_run_id", runID), zap.String("recovery_id", recovery.RecoveryID), zap.Error(err))
			continue
		}
		log.Logger.Info("portfolio_recovery.completed",
			zap.String("refresh_run_id", runID),
			zap.String("recovery_id", recovery.RecoveryID),
			zap.String("portfolio_id", recovery.Reference.PortfolioID),
			zap.String("portfolio_ticker", recovery.Reference.PortfolioTicker),
		)
	}
}

func (s *TradeScheduler) reportPortfolioRecoveryFailure(ctx context.Context, runID string, recovery PortfolioRecovery, err error) {
	threshold := s.cfg.GemReportAfterFailures
	if threshold <= 0 {
		threshold = 1
	}
	if s.gem == nil || recovery.Attempts < threshold || recovery.Attempts%threshold != 0 {
		return
	}
	payload := fmt.Sprintf(
		"phase=portfolio_recovery;recovery_id=%s;portfolio_id=%s;portfolio_ticker=%s;window_start_pt=%s;window_end_pt=%s;attempt=%d",
		recovery.RecoveryID,
		recovery.Reference.PortfolioID,
		recovery.Reference.PortfolioTicker,
		pacificLogTimestamp(recovery.Start),
		pacificLogTimestamp(recovery.End),
		recovery.Attempts,
	)
	gemCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, _ = s.gem.Report(gemCtx, buildTradeRefreshGemRequest(runID, ApiException, "Removed portfolio recovery remains unresolved", err, payload))
}

func (s *TradeScheduler) filterRequestBody(windowStart, windowEnd time.Time, pageToken string) ([]byte, error) {
	return s.filterRequestBodyFor(windowStart, windowEnd, pageToken, s.cfg.PortfolioFilter)
}

func (s *TradeScheduler) filterRequestBodyFor(windowStart, windowEnd time.Time, pageToken string, filter PortfolioFilter) ([]byte, error) {
	duration := windowEnd.Sub(windowStart)
	if duration <= 0 || duration > maxTradeFilterDuration {
		return nil, fmt.Errorf("trade filter modify-time range must be greater than zero and no longer than %s; got %s", maxTradeFilterDuration, duration)
	}
	request := map[string]any{
		"pageSize": s.cfg.PageSize,
		"expands":  []string{"asset.summary"},
		"query": map[string]any{
			"criteria": map[string]any{
				"portfolio": filter.Criteria(),
				"dateTime": map[string]any{
					"modifyTimeRange": map[string]any{
						"startTime": windowStart.UTC().Format(time.RFC3339Nano),
						"endTime":   windowEnd.UTC().Format(time.RFC3339Nano),
					},
				},
			},
			"options": []string{
				"FILTER_QUERY_OPTION_LOAD_TRADE_RELATIONSHIP",
				"FILTER_QUERY_OPTION_LOAD_EXTERNAL_TRADE_REFERENCE",
				"FILTER_QUERY_OPTION_LOAD_TRADE_COLLATERAL",
				"FILTER_QUERY_OPTION_LOAD_TRADE_FX_LEGS",
				"FILTER_QUERY_OPTION_LOAD_ORIGINAL_ORDER_REFERENCE",
			},
		},
	}
	if pageToken != "" {
		request["pageToken"] = pageToken
	}
	return json.Marshal(request)
}

func (s *TradeScheduler) inBusinessWindow(now time.Time) bool {
	local := now.In(pacificLocation())
	if local.Weekday() == time.Saturday || local.Weekday() == time.Sunday {
		return false
	}
	minute := local.Hour()*60 + local.Minute()
	return minute >= s.businessStartMinutePT() && minute < s.businessEndMinutePT()
}

func (s *TradeScheduler) businessStartMinutePT() int {
	if s.cfg.BusinessEndMinutePT <= s.cfg.BusinessStartMinutePT {
		return defaultBusinessStartMinute
	}
	return s.cfg.BusinessStartMinutePT
}

func (s *TradeScheduler) businessEndMinutePT() int {
	if s.cfg.BusinessEndMinutePT <= s.cfg.BusinessStartMinutePT {
		return defaultBusinessEndMinute
	}
	return s.cfg.BusinessEndMinutePT
}

func formatMinuteOfDay(minute int) string {
	return fmt.Sprintf("%02d:%02d", minute/60, minute%60)
}

func (s *TradeScheduler) reportFailure(ctx context.Context, correlationID string, exceptionName ExceptionNameEnum, message string, err error, payload string) {
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	s.consecutiveFailures++
	fields := []zap.Field{
		zap.String("correlation_id", correlationID),
		zap.String("refresh_run_id", correlationID),
		zap.String("message", message),
		zap.String("exception_name", string(exceptionName)),
		zap.Int("consecutive_failures", s.consecutiveFailures),
		zap.String("failure_context", payload),
		zap.Error(err),
	}
	fields = append(fields, refreshFailureLogFields(err)...)
	log.Logger.Error("refresh.run.failed", fields...)
	threshold := s.cfg.GemReportAfterFailures
	if threshold <= 0 {
		threshold = 1
	}
	if s.gem != nil && s.consecutiveFailures >= threshold {
		gemCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if _, reportErr := s.gem.Report(gemCtx, buildTradeRefreshGemRequest(correlationID, exceptionName, message, err, payload)); reportErr == nil {
			s.consecutiveFailures = 0
		}
	}
}

func refreshFailureLogFields(err error) []zap.Field {
	fields := []zap.Field{zap.String("error_type", fmt.Sprintf("%T", err))}
	var upstream *blackRockFilterError
	if errors.As(err, &upstream) {
		fields = append(fields,
			zap.Int("upstream_status_code", upstream.StatusCode),
			zap.String("blackrock_request_id", upstream.RequestID),
			zap.Int("api_attempts", upstream.Attempts),
			zap.Int("portfolio_count", upstream.PortfolioCount),
			zap.String("window_start_pt", pacificLogTimestamp(upstream.Window.Start)),
			zap.String("window_end_pt", pacificLogTimestamp(upstream.Window.End)),
			zap.String("upstream_error", upstream.Summary),
			zap.Bool("permanent_portfolio_error", isPermanentPortfolioFilterError(err)),
		)
	}
	var status *tradeFilterStatusError
	if errors.As(err, &status) {
		fields = append(fields,
			zap.Int("embedded_status_code", status.Code),
			zap.String("embedded_status_message", status.Message),
			zap.Int("embedded_status_details", status.DetailsCount),
			zap.Int("processed_result_count", status.Processed),
			zap.Int("expected_result_count", status.Total),
		)
	}
	var mergeErr *snowflakePageMergeError
	if errors.As(err, &mergeErr) {
		fields = append(fields, zap.Int("snowflake_page_number", mergeErr.page))
	}
	return fields
}

func buildTradeRefreshGemRequest(correlationID string, exceptionName ExceptionNameEnum, message string, err error, payload string) *GemReportRequest {
	rb := NewGemRequestBuilder(correlationID)
	rb.WithException().
		TypeName("Technical_Exception").
		SeverityName("High").
		Name(exceptionName).
		Message(message + ": " + err.Error()).
		ActualDate(time.Now()).
		Source("Aladdin Trade API").
		Target("Trade Sink - Snowflake").
		Workflow("Trade Refresh").
		WithTransactor().
		Service("Trade Refresher").
		Version("1.0.0").
		Function("refresh").
		User("N/A").
		Computer("N/A").
		WithDetails().
		StackTrace(err.Error()).
		Payload(payload)

	return rb.Build()
}
