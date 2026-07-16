package services

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
	"go.uber.org/zap"

	"refresher/trade-refresher/internal/utils/azure"
	"refresher/trade-refresher/internal/utils/log"
	"refresher/trade-refresher/internal/utils/ptime"
)

const maxSnowflakeJSONBatchBytes = 8 << 20

type SnowflakeConfig struct {
	Account            string
	User               string
	Role               string
	Warehouse          string
	Database           string
	Schema             string
	Authenticator      string
	KeepSessionAlive   bool
	DerSecretName      string
	PasswordSecretName string
	TradeTable         string
	StateTable         string
	InsertBatchSize    int
	MaxOpenConns       int
	MaxIdleConns       int
	ConnectionTTL      time.Duration
}

type CheckpointState struct {
	LastSuccessfulEnd       time.Time
	PortfolioFilterKey      string
	PortfolioGroupTicker    string
	PortfolioGroupMembers   []PortfolioReference
	PortfolioGroupFetchedAt time.Time
}

type RefreshRun struct {
	RunID                   string
	ProcessName             string
	LockOwner               string
	LockToken               string
	WindowStart             time.Time
	WindowEnd               time.Time
	PageCount               int
	RowsWritten             int
	PortfolioFilterKey      string
	PortfolioGroupTicker    string
	PortfolioGroupMembers   []PortfolioReference
	PortfolioGroupFetchedAt time.Time
	NewRecoveries           []PortfolioRecovery
}

type PortfolioRecovery struct {
	RecoveryID  string
	Reference   PortfolioReference
	Start       time.Time
	End         time.Time
	NextAttempt time.Time
	Attempts    int
	LastError   string
}

type MergePageResult struct {
	BatchCount   int
	PayloadBytes int
}

type SnowflakeStore struct {
	db           *sql.DB
	cfg          SnowflakeConfig
	mergeSQLOnce sync.Once
	mergeSQL     string
}

func NewSnowflakeStore(ctx context.Context, cfg SnowflakeConfig, vault azure.Vault) (*SnowflakeStore, error) {
	if cfg.Account == "" || cfg.User == "" || cfg.Database == "" || cfg.Schema == "" || cfg.TradeTable == "" || cfg.StateTable == "" {
		return nil, errors.New("snowflake configuration is incomplete")
	}
	authType, err := parseSnowflakeAuthType(cfg.Authenticator)
	if err != nil {
		return nil, err
	}
	privateKey, err := snowflakePrivateKey(authType, cfg, vault)
	if err != nil {
		return nil, err
	}
	sfConfig := gosnowflake.Config{
		Account:          cfg.Account,
		User:             cfg.User,
		Role:             cfg.Role,
		Warehouse:        cfg.Warehouse,
		Database:         cfg.Database,
		Schema:           cfg.Schema,
		Authenticator:    authType,
		PrivateKey:       privateKey,
		KeepSessionAlive: cfg.KeepSessionAlive,
		Params:           snowflakeSessionParameters(),
	}
	dsn, err := gosnowflake.DSN(&sfConfig)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}
	configureConnectionPool(db, cfg)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &SnowflakeStore{db: db, cfg: cfg}
	if err := store.ensureTables(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize Snowflake tables: %w", err)
	}
	return store, nil
}

func snowflakePrivateKey(authType gosnowflake.AuthType, cfg SnowflakeConfig, vault azure.Vault) (*rsa.PrivateKey, error) {
	if authType != gosnowflake.AuthTypeJwt {
		return nil, nil
	}
	if vault == nil {
		return nil, errors.New("key vault is required for Snowflake JWT authentication")
	}
	derValue, err := vault.Get(cfg.DerSecretName)
	if err != nil {
		return nil, err
	}
	decodedDER, err := base64.StdEncoding.DecodeString(derValue)
	if err != nil {
		return nil, fmt.Errorf("decode Snowflake DER secret: %w", err)
	}
	password, err := vault.Get(cfg.PasswordSecretName)
	if err != nil {
		return nil, err
	}
	privateKey, err := pkcs8.ParsePKCS8PrivateKeyRSA(decodedDER, []byte(password))
	if err != nil {
		return nil, fmt.Errorf("parse Snowflake private key: %w", err)
	}
	return privateKey, nil
}

func configureConnectionPool(db *sql.DB, cfg SnowflakeConfig) {
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnectionTTL > 0 {
		db.SetConnMaxLifetime(cfg.ConnectionTTL)
	}
}

func NewSnowflakeStoreWithDB(db *sql.DB, cfg SnowflakeConfig) *SnowflakeStore {
	return &SnowflakeStore{db: db, cfg: cfg}
}

func (s *SnowflakeStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SnowflakeStore) ensureTables(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, createTradeTableSQL(s.qualified(s.cfg.TradeTable))); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, createStateTableSQL(s.qualified(s.cfg.StateTable))); err != nil {
		return err
	}
	for _, statement := range stateTableMigrationSQL(s.qualified(s.cfg.StateTable)) {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	tradeColumns := make([]string, 0, len(tradeTableDefinition().Columns))
	for _, column := range tradeTableDefinition().Columns {
		tradeColumns = append(tradeColumns, column.Name)
	}
	if err := s.validateTableColumns(ctx, s.cfg.TradeTable, tradeColumns); err != nil {
		return err
	}
	if err := s.validateTimestampColumns(ctx, s.cfg.TradeTable, tradeTableDefinition().Columns); err != nil {
		return err
	}
	if err := s.validateTableColumns(ctx, s.cfg.StateTable, []string{
		"PROCESS_NAME", "LAST_SUCCESSFUL_END_PT", "LOCK_OWNER", "LOCK_TOKEN", "LOCK_EXPIRES_AT_PT",
		"LAST_RUN_ID", "LAST_ROWS_WRITTEN", "PORTFOLIO_FILTER_KEY", "PORTFOLIO_GROUP_TICKER", "PORTFOLIO_GROUP_MEMBERS",
		"PORTFOLIO_GROUP_FETCHED_AT_PT", "RECOVERY_PORTFOLIO_ID", "RECOVERY_PORTFOLIO_TICKER", "RECOVERY_START_PT",
		"RECOVERY_END_PT", "RECOVERY_NEXT_ATTEMPT_PT", "RECOVERY_ATTEMPTS", "RECOVERY_LAST_ERROR", "UPDATED_AT_PT",
	}); err != nil {
		return err
	}
	if err := s.validateTimestampColumns(ctx, s.cfg.StateTable, []ColumnDefinition{
		column("LAST_SUCCESSFUL_END_PT", "TIMESTAMP_TZ", true),
		column("LOCK_EXPIRES_AT_PT", "TIMESTAMP_TZ", true),
		column("PORTFOLIO_GROUP_FETCHED_AT_PT", "TIMESTAMP_TZ", true),
		column("RECOVERY_START_PT", "TIMESTAMP_TZ", true),
		column("RECOVERY_END_PT", "TIMESTAMP_TZ", true),
		column("RECOVERY_NEXT_ATTEMPT_PT", "TIMESTAMP_TZ", true),
		column("UPDATED_AT_PT", "TIMESTAMP_TZ", true),
	}); err != nil {
		return err
	}
	query := fmt.Sprintf(`
MERGE INTO %s target
USING (SELECT ? AS PROCESS_NAME) source
ON target.PROCESS_NAME = source.PROCESS_NAME
WHEN NOT MATCHED THEN INSERT (PROCESS_NAME, UPDATED_AT_PT)
VALUES (source.PROCESS_NAME, %s)`, s.qualified(s.cfg.StateTable), snowflakeCurrentPacificTimestamp())
	_, err := s.db.ExecContext(ctx, query, tradeRefreshProcessName)
	if err != nil {
		return err
	}
	var stateRows int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE PROCESS_NAME = ?", s.qualified(s.cfg.StateTable))
	if err := s.db.QueryRowContext(ctx, countQuery, tradeRefreshProcessName).Scan(&stateRows); err != nil {
		return err
	}
	if stateRows != 1 {
		return fmt.Errorf("snowflake state table must contain exactly one %q row; found %d", tradeRefreshProcessName, stateRows)
	}
	return nil
}

func (s *SnowflakeStore) validateTimestampColumns(ctx context.Context, table string, definitions []ColumnDefinition) error {
	expected := make(map[string]struct{})
	for _, definition := range definitions {
		if definition.IsDateTime {
			expected[definition.Name] = struct{}{}
		}
	}
	query := fmt.Sprintf(`SELECT COLUMN_NAME, DATA_TYPE FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, quoteIdent(s.cfg.Database))
	rows, err := s.db.QueryContext(ctx, query, strings.ToUpper(s.cfg.Schema), strings.ToUpper(table))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			return err
		}
		if _, ok := expected[strings.ToUpper(name)]; ok && !strings.EqualFold(dataType, "TIMESTAMP_TZ") {
			return fmt.Errorf("snowflake column %s.%s.%s.%s must be TIMESTAMP_TZ; found %s", s.cfg.Database, s.cfg.Schema, table, name, dataType)
		}
	}
	return rows.Err()
}

func (s *SnowflakeStore) validateTableColumns(ctx context.Context, table string, expected []string) error {
	query := fmt.Sprintf(`SELECT COLUMN_NAME FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, quoteIdent(s.cfg.Database))
	rows, err := s.db.QueryContext(ctx, query, strings.ToUpper(s.cfg.Schema), strings.ToUpper(table))
	if err != nil {
		return err
	}
	defer rows.Close()
	actual := make(map[string]struct{}, len(expected))
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return err
		}
		actual[strings.ToUpper(column)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var missing []string
	for _, column := range expected {
		if _, ok := actual[column]; !ok {
			missing = append(missing, column)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("snowflake table %s.%s.%s is missing columns: %s", s.cfg.Database, s.cfg.Schema, table, strings.Join(missing, ", "))
	}
	return nil
}

func (s *SnowflakeStore) LoadCheckpoint(ctx context.Context, processName string) (CheckpointState, error) {
	var lastSuccessfulEnd, portfolioGroupFetchedAt sql.NullTime
	var portfolioFilterKey, portfolioGroupTicker, portfolioGroupMembersJSON string
	query := fmt.Sprintf(`SELECT LAST_SUCCESSFUL_END_PT,
       COALESCE(PORTFOLIO_FILTER_KEY, ''),
       COALESCE(PORTFOLIO_GROUP_TICKER, ''),
       COALESCE(TO_JSON(PORTFOLIO_GROUP_MEMBERS), '[]'),
       PORTFOLIO_GROUP_FETCHED_AT_PT
FROM %s WHERE PROCESS_NAME = ?`, s.qualified(s.cfg.StateTable))
	err := s.db.QueryRowContext(ctx, query, processName).Scan(
		&lastSuccessfulEnd,
		&portfolioFilterKey,
		&portfolioGroupTicker,
		&portfolioGroupMembersJSON,
		&portfolioGroupFetchedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return CheckpointState{}, nil
	}
	if err != nil {
		return CheckpointState{}, err
	}
	portfolioGroupMembers, err := decodePortfolioGroupMembers([]byte(portfolioGroupMembersJSON))
	if err != nil {
		return CheckpointState{}, fmt.Errorf("decode portfolio group checkpoint members: %w", err)
	}
	state := CheckpointState{
		PortfolioFilterKey:    portfolioFilterKey,
		PortfolioGroupTicker:  portfolioGroupTicker,
		PortfolioGroupMembers: portfolioGroupMembers,
	}
	if lastSuccessfulEnd.Valid {
		state.LastSuccessfulEnd = lastSuccessfulEnd.Time
	}
	if portfolioGroupFetchedAt.Valid {
		state.PortfolioGroupFetchedAt = portfolioGroupFetchedAt.Time
	}
	return state, nil
}

func (s *SnowflakeStore) AcquireLease(ctx context.Context, processName, owner, token string, ttl time.Duration) (bool, error) {
	query := fmt.Sprintf(`
UPDATE %s
SET LOCK_OWNER = ?, LOCK_TOKEN = ?, LOCK_EXPIRES_AT_PT = %s, UPDATED_AT_PT = %s
WHERE PROCESS_NAME = ?
  AND (LOCK_TOKEN IS NULL OR LOCK_EXPIRES_AT_PT IS NULL OR LOCK_EXPIRES_AT_PT < %s)`,
		s.qualified(s.cfg.StateTable), snowflakePacificTimestampAfterSeconds(), snowflakeCurrentPacificTimestamp(), snowflakeCurrentPacificTimestamp())
	result, err := s.db.ExecContext(ctx, query, owner, token, int(ttl.Seconds()), processName)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected == 1, nil
}

func (s *SnowflakeStore) ExtendLease(ctx context.Context, processName, owner, token string, ttl time.Duration) error {
	query := fmt.Sprintf(`
UPDATE %s
SET LOCK_EXPIRES_AT_PT = %s, UPDATED_AT_PT = %s
WHERE PROCESS_NAME = ? AND LOCK_OWNER = ? AND LOCK_TOKEN = ?`,
		s.qualified(s.cfg.StateTable), snowflakePacificTimestampAfterSeconds(), snowflakeCurrentPacificTimestamp())
	result, err := s.db.ExecContext(ctx, query, int(ttl.Seconds()), processName, owner, token)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return errors.New("snowflake lease was lost")
	}
	return nil
}

func (s *SnowflakeStore) ReleaseLease(ctx context.Context, processName, owner, token string) error {
	query := fmt.Sprintf(`
UPDATE %s
SET LOCK_OWNER = NULL, LOCK_TOKEN = NULL, LOCK_EXPIRES_AT_PT = NULL, UPDATED_AT_PT = %s
WHERE PROCESS_NAME = ? AND LOCK_OWNER = ? AND LOCK_TOKEN = ?`,
		s.qualified(s.cfg.StateTable), snowflakeCurrentPacificTimestamp())
	_, err := s.db.ExecContext(ctx, query, processName, owner, token)
	return err
}

func (s *SnowflakeStore) MergePage(ctx context.Context, runID string, pageNumber int, dataset TradeDataset) (MergePageResult, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return MergePageResult{}, err
	}
	defer conn.Close()

	queryTag := fmt.Sprintf(`{"service":"trade-refresher","refresh_run_id":"%s","operation":"merge_trade_page","page":%d}`, runID, pageNumber)
	if _, err := conn.ExecContext(ctx, "ALTER SESSION SET QUERY_TAG = '"+escapeSQLString(queryTag)+"'"); err != nil {
		return MergePageResult{}, err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return MergePageResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				log.Logger.Error("snowflake.transaction.rollback_failed", zap.Error(rollbackErr))
			}
		}
	}()

	batchCount, payloadBytes, err := s.mergeRows(ctx, tx, dataset.Rows)
	if err != nil {
		return MergePageResult{}, fmt.Errorf("merge trades: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return MergePageResult{}, err
	}
	committed = true
	return MergePageResult{BatchCount: batchCount, PayloadBytes: payloadBytes}, nil
}

func (s *SnowflakeStore) CommitCheckpoint(ctx context.Context, run RefreshRun) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	queryTag := fmt.Sprintf(`{"service":"trade-refresher","refresh_run_id":"%s","operation":"commit_checkpoint"}`, run.RunID)
	if _, err := conn.ExecContext(ctx, "ALTER SESSION SET QUERY_TAG = '"+escapeSQLString(queryTag)+"'"); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := s.upsertPortfolioRecoveries(ctx, tx, run.NewRecoveries); err != nil {
		return err
	}
	if err := s.updateCheckpoint(ctx, tx, run); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *SnowflakeStore) LoadDuePortfolioRecoveries(ctx context.Context) ([]PortfolioRecovery, error) {
	query := fmt.Sprintf(`SELECT PROCESS_NAME, COALESCE(RECOVERY_PORTFOLIO_ID, ''), COALESCE(RECOVERY_PORTFOLIO_TICKER, ''),
       RECOVERY_START_PT, RECOVERY_END_PT, RECOVERY_NEXT_ATTEMPT_PT,
       COALESCE(RECOVERY_ATTEMPTS, 0), COALESCE(RECOVERY_LAST_ERROR, '')
FROM %s
WHERE STARTSWITH(PROCESS_NAME, 'trade_refresh_recovery:')
  AND RECOVERY_START_PT IS NOT NULL
  AND RECOVERY_END_PT IS NOT NULL
  AND (RECOVERY_NEXT_ATTEMPT_PT IS NULL OR RECOVERY_NEXT_ATTEMPT_PT <= %s)
ORDER BY RECOVERY_NEXT_ATTEMPT_PT NULLS FIRST, RECOVERY_START_PT
LIMIT 5`, s.qualified(s.cfg.StateTable), snowflakeCurrentPacificTimestamp())
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	recoveries := make([]PortfolioRecovery, 0)
	for rows.Next() {
		var recovery PortfolioRecovery
		var nextAttempt sql.NullTime
		if err := rows.Scan(
			&recovery.RecoveryID,
			&recovery.Reference.PortfolioID,
			&recovery.Reference.PortfolioTicker,
			&recovery.Start,
			&recovery.End,
			&nextAttempt,
			&recovery.Attempts,
			&recovery.LastError,
		); err != nil {
			return nil, err
		}
		if nextAttempt.Valid {
			recovery.NextAttempt = nextAttempt.Time
		}
		recoveries = append(recoveries, recovery)
	}
	return recoveries, rows.Err()
}

func (s *SnowflakeStore) CompletePortfolioRecovery(ctx context.Context, recoveryID string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE PROCESS_NAME = ?", s.qualified(s.cfg.StateTable))
	_, err := s.db.ExecContext(ctx, query, recoveryID)
	return err
}

func (s *SnowflakeStore) ReschedulePortfolioRecovery(ctx context.Context, recovery PortfolioRecovery) error {
	query := fmt.Sprintf(`UPDATE %s
SET RECOVERY_START_PT = ?, RECOVERY_END_PT = ?, RECOVERY_NEXT_ATTEMPT_PT = ?,
    RECOVERY_ATTEMPTS = ?, RECOVERY_LAST_ERROR = ?, UPDATED_AT_PT = %s
WHERE PROCESS_NAME = ?`, s.qualified(s.cfg.StateTable), snowflakeCurrentPacificTimestamp())
	_, err := s.db.ExecContext(
		ctx,
		query,
		snowflakePacificTimestamp(recovery.Start),
		snowflakePacificTimestamp(recovery.End),
		snowflakePacificTimestamp(recovery.NextAttempt),
		recovery.Attempts,
		truncateRecoveryError(recovery.LastError),
		recovery.RecoveryID,
	)
	return err
}

func (s *SnowflakeStore) mergeRows(ctx context.Context, tx *sql.Tx, rows []TradeTableRow) (int, int, error) {
	if len(rows) == 0 {
		return 0, 0, nil
	}
	batchSize := s.cfg.InsertBatchSize
	if batchSize <= 0 {
		batchSize = 5000
	}
	query := s.tradeMergeSQL()
	batchCount := 0
	payloadBytes := 0
	for start := 0; start < len(rows); {
		end, payload, err := marshalTradeBatch(rows, start, batchSize)
		if err != nil {
			return batchCount, payloadBytes, err
		}
		if _, err := tx.ExecContext(ctx, query, string(payload)); err != nil {
			return batchCount, payloadBytes, err
		}
		batchCount++
		payloadBytes += len(payload)
		start = end
	}
	return batchCount, payloadBytes, nil
}

func (s *SnowflakeStore) tradeMergeSQL() string {
	s.mergeSQLOnce.Do(func() {
		s.mergeSQL = mergeTradesSQL(s.qualified(s.cfg.TradeTable), tradeTableDefinition())
	})
	return s.mergeSQL
}

func marshalTradeBatch(rows []TradeTableRow, start, maxRows int) (int, []byte, error) {
	if start < 0 || start >= len(rows) {
		return start, nil, fmt.Errorf("snowflake batch start %d is outside %d rows", start, len(rows))
	}
	if maxRows < 1 {
		maxRows = 1
	}
	definition := tradeTableDefinition()
	limit := start + maxRows
	if limit > len(rows) {
		limit = len(rows)
	}
	payload := make([]byte, 1, min(maxSnowflakeJSONBatchBytes, maxRows*1024))
	payload[0] = '['
	end := start
	for end < limit {
		encoded, err := json.Marshal(snowflakeJSONRowWithDefinition(rows[end].Values, definition))
		if err != nil {
			return start, nil, err
		}
		separatorBytes := 0
		if end > start {
			separatorBytes = 1
		}
		if end > start && len(payload)+separatorBytes+len(encoded)+1 > maxSnowflakeJSONBatchBytes {
			break
		}
		if end == start && len(payload)+len(encoded)+1 > maxSnowflakeJSONBatchBytes {
			return start, nil, fmt.Errorf("one trade row exceeds the %d-byte Snowflake batch limit", maxSnowflakeJSONBatchBytes)
		}
		if separatorBytes > 0 {
			payload = append(payload, ',')
		}
		payload = append(payload, encoded...)
		end++
	}
	payload = append(payload, ']')
	return end, payload, nil
}

func snowflakeJSONRowWithDefinition(values map[string]any, definition TradeTableDefinition) map[string]any {
	row := make(map[string]any, len(values))
	for name, value := range values {
		column, exists := definition.columnByName[name]
		if !exists || value == nil {
			continue
		}
		if column.IsPayload {
			if payload, ok := value.(string); ok && json.Valid([]byte(payload)) {
				value = json.RawMessage(payload)
			}
		}
		if column.IsDateTime {
			if timestamp, ok := value.(time.Time); ok && !timestamp.IsZero() {
				value = pacificLogTimestamp(timestamp)
			}
		}
		row[name] = normalizeSnowflakeScalar(value)
	}
	return row
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *SnowflakeStore) updateCheckpoint(ctx context.Context, executor sqlExecutor, run RefreshRun) error {
	members := run.PortfolioGroupMembers
	if members == nil {
		members = []PortfolioReference{}
	}
	membersJSON, err := json.Marshal(members)
	if err != nil {
		return fmt.Errorf("encode portfolio group checkpoint members: %w", err)
	}
	var fetchedAt any
	if !run.PortfolioGroupFetchedAt.IsZero() {
		fetchedAt = snowflakePacificTimestamp(run.PortfolioGroupFetchedAt)
	}
	query := fmt.Sprintf(`
UPDATE %s
SET LAST_SUCCESSFUL_END_PT = ?, LOCK_OWNER = NULL, LOCK_TOKEN = NULL, LOCK_EXPIRES_AT_PT = NULL,
    LAST_RUN_ID = ?, LAST_ROWS_WRITTEN = ?, PORTFOLIO_FILTER_KEY = NULLIF(?, ''),
    PORTFOLIO_GROUP_TICKER = NULLIF(?, ''),
    PORTFOLIO_GROUP_MEMBERS = PARSE_JSON(?), PORTFOLIO_GROUP_FETCHED_AT_PT = ?, UPDATED_AT_PT = %s
WHERE PROCESS_NAME = ? AND LOCK_OWNER = ? AND LOCK_TOKEN = ?`,
		s.qualified(s.cfg.StateTable), snowflakeCurrentPacificTimestamp())
	result, err := executor.ExecContext(
		ctx,
		query,
		snowflakePacificTimestamp(run.WindowEnd),
		run.RunID,
		run.RowsWritten,
		run.PortfolioFilterKey,
		run.PortfolioGroupTicker,
		string(membersJSON),
		fetchedAt,
		run.ProcessName,
		run.LockOwner,
		run.LockToken,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return errors.New("checkpoint update failed because lease was lost")
	}
	return nil
}

func stateTableMigrationSQL(table string) []string {
	return []string{
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS PORTFOLIO_FILTER_KEY VARCHAR", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS PORTFOLIO_GROUP_TICKER VARCHAR", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS PORTFOLIO_GROUP_MEMBERS VARIANT", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS PORTFOLIO_GROUP_FETCHED_AT_PT TIMESTAMP_TZ", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS RECOVERY_PORTFOLIO_ID VARCHAR", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS RECOVERY_PORTFOLIO_TICKER VARCHAR", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS RECOVERY_START_PT TIMESTAMP_TZ", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS RECOVERY_END_PT TIMESTAMP_TZ", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS RECOVERY_NEXT_ATTEMPT_PT TIMESTAMP_TZ", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS RECOVERY_ATTEMPTS NUMBER(38,0)", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS RECOVERY_LAST_ERROR VARCHAR", table),
	}
}

func (s *SnowflakeStore) upsertPortfolioRecoveries(ctx context.Context, executor sqlExecutor, recoveries []PortfolioRecovery) error {
	if len(recoveries) == 0 {
		return nil
	}
	query := fmt.Sprintf(`MERGE INTO %s target
USING (SELECT ? AS PROCESS_NAME) source
ON target.PROCESS_NAME = source.PROCESS_NAME
WHEN MATCHED THEN UPDATE SET
  RECOVERY_NEXT_ATTEMPT_PT = ?, RECOVERY_ATTEMPTS = ?, RECOVERY_LAST_ERROR = ?, UPDATED_AT_PT = %s
WHEN NOT MATCHED THEN INSERT (
  PROCESS_NAME, RECOVERY_PORTFOLIO_ID, RECOVERY_PORTFOLIO_TICKER, RECOVERY_START_PT, RECOVERY_END_PT,
  RECOVERY_NEXT_ATTEMPT_PT, RECOVERY_ATTEMPTS, RECOVERY_LAST_ERROR, UPDATED_AT_PT
) VALUES (?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, %s)`,
		s.qualified(s.cfg.StateTable), snowflakeCurrentPacificTimestamp(), snowflakeCurrentPacificTimestamp())
	for _, recovery := range recoveries {
		if _, err := executor.ExecContext(
			ctx,
			query,
			recovery.RecoveryID,
			snowflakePacificTimestamp(recovery.NextAttempt),
			recovery.Attempts,
			truncateRecoveryError(recovery.LastError),
			recovery.RecoveryID,
			recovery.Reference.PortfolioID,
			recovery.Reference.PortfolioTicker,
			snowflakePacificTimestamp(recovery.Start),
			snowflakePacificTimestamp(recovery.End),
			snowflakePacificTimestamp(recovery.NextAttempt),
			recovery.Attempts,
			truncateRecoveryError(recovery.LastError),
		); err != nil {
			return fmt.Errorf("persist portfolio recovery %q: %w", recovery.RecoveryID, err)
		}
	}
	return nil
}

func decodePortfolioGroupMembers(body []byte) ([]PortfolioReference, error) {
	var members []PortfolioReference
	if err := json.Unmarshal(body, &members); err == nil {
		return members, nil
	}
	var legacy []string
	if err := json.Unmarshal(body, &legacy); err != nil {
		return nil, err
	}
	members = make([]PortfolioReference, 0, len(legacy))
	for _, ticker := range legacy {
		members = append(members, PortfolioReference{PortfolioTicker: ticker})
	}
	return members, nil
}

func truncateRecoveryError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 1000 {
		return value[:1000]
	}
	return value
}

func (s *SnowflakeStore) qualified(table string) string {
	return fmt.Sprintf("%s.%s.%s", quoteIdent(s.cfg.Database), quoteIdent(s.cfg.Schema), quoteIdent(table))
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func snowflakeCurrentPacificTimestamp() string {
	return "CONVERT_TIMEZONE('America/Los_Angeles', CURRENT_TIMESTAMP())"
}

func snowflakePacificTimestampAfterSeconds() string {
	return "CONVERT_TIMEZONE('America/Los_Angeles', DATEADD(second, ?, CURRENT_TIMESTAMP()))"
}

func snowflakeSessionParameters() map[string]*string {
	timezone := ptime.PacificTimeZone
	return map[string]*string{"timezone": &timezone}
}

func normalizeSnowflakeScalar(value any) any {
	switch typed := value.(type) {
	case json.Number:
		return typed.String()
	default:
		return typed
	}
}

func parseSnowflakeAuthType(authType string) (gosnowflake.AuthType, error) {
	switch strings.ToUpper(authType) {
	case "SNOWFLAKE_JWT", "":
		return gosnowflake.AuthTypeJwt, nil
	case "EXTERNALBROWSER":
		return gosnowflake.AuthTypeExternalBrowser, nil
	default:
		return -1, fmt.Errorf("unsupported Snowflake authenticator %q", authType)
	}
}
