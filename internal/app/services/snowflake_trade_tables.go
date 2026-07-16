package services

import (
	"fmt"
	"strings"
	"sync"
)

func createTradeTableSQL(table string) string {
	definition := tradeTableDefinition()
	columns := make([]string, 0, len(definition.Columns)+1)
	for _, column := range definition.Columns {
		nullability := ""
		if column.Name == "TRADE_CURRENT_KEY" || column.Name == "TRADE_VERSION_KEY" {
			nullability = " NOT NULL"
		}
		comment := ""
		if column.Comment != "" {
			comment = " COMMENT '" + escapeSQLString(column.Comment) + "'"
		}
		columns = append(columns, fmt.Sprintf("%s %s%s%s", quoteIdent(column.Name), column.SQLType, nullability, comment))
	}
	columns = append(columns, "CONSTRAINT PK_TRADES PRIMARY KEY (TRADE_CURRENT_KEY) NOT ENFORCED")
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n) DATA_RETENTION_TIME_IN_DAYS = 7 CHANGE_TRACKING = FALSE", table, strings.Join(columns, ",\n  "))
}

func createStateTableSQL(table string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  PROCESS_NAME VARCHAR NOT NULL,
  LAST_SUCCESSFUL_END_PT TIMESTAMP_TZ,
  LOCK_OWNER VARCHAR,
  LOCK_TOKEN VARCHAR,
  LOCK_EXPIRES_AT_PT TIMESTAMP_TZ,
  LAST_RUN_ID VARCHAR,
  LAST_ROWS_WRITTEN NUMBER(38,0),
  PORTFOLIO_FILTER_KEY VARCHAR,
  PORTFOLIO_GROUP_TICKER VARCHAR,
  PORTFOLIO_GROUP_MEMBERS VARIANT,
  PORTFOLIO_GROUP_FETCHED_AT_PT TIMESTAMP_TZ,
  RECOVERY_PORTFOLIO_ID VARCHAR,
  RECOVERY_PORTFOLIO_TICKER VARCHAR,
  RECOVERY_START_PT TIMESTAMP_TZ,
  RECOVERY_END_PT TIMESTAMP_TZ,
  RECOVERY_NEXT_ATTEMPT_PT TIMESTAMP_TZ,
  RECOVERY_ATTEMPTS NUMBER(38,0),
  RECOVERY_LAST_ERROR VARCHAR,
  UPDATED_AT_PT TIMESTAMP_TZ,
  CONSTRAINT PK_TRADE_REFRESH_STATE PRIMARY KEY (PROCESS_NAME) NOT ENFORCED
) DATA_RETENTION_TIME_IN_DAYS = 7 CHANGE_TRACKING = FALSE`, table)
}

func mergeTradesSQL(table string, definition TradeTableDefinition) string {
	expressions := make([]string, 0, len(definition.Columns))
	updates := make([]string, 0, len(definition.Columns)-1)
	insertColumns := make([]string, 0, len(definition.Columns))
	insertValues := make([]string, 0, len(definition.Columns))
	for _, column := range definition.Columns {
		expressions = append(expressions, sourceColumnExpression(column))
		insertColumns = append(insertColumns, quoteIdent(column.Name))
		insertValues = append(insertValues, "source."+quoteIdent(column.Name))
		if column.Name != "TRADE_CURRENT_KEY" {
			updates = append(updates, "target."+quoteIdent(column.Name)+" = source."+quoteIdent(column.Name))
		}
	}
	return fmt.Sprintf(`MERGE INTO %s target
USING (
  SELECT
    %s
  FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)))
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY value:"TRADE_CURRENT_KEY"::VARCHAR
    ORDER BY TRY_TO_TIMESTAMP_TZ(value:"MODIFY_TIME_PT"::VARCHAR) DESC NULLS LAST,
	         TRY_TO_TIMESTAMP_TZ(value:"INGESTED_AT_PT"::VARCHAR) DESC NULLS LAST,
	         value:"PAGE_NUMBER"::NUMBER DESC NULLS LAST
  ) = 1
) source
ON target.TRADE_CURRENT_KEY = source.TRADE_CURRENT_KEY
WHEN MATCHED
  AND target.TRADE_VERSION_KEY <> source.TRADE_VERSION_KEY
  AND (
    (source.MODIFY_TIME_PT IS NOT NULL AND target.MODIFY_TIME_PT IS NULL)
    OR source.MODIFY_TIME_PT > target.MODIFY_TIME_PT
    OR (
      (source.MODIFY_TIME_PT = target.MODIFY_TIME_PT OR (source.MODIFY_TIME_PT IS NULL AND target.MODIFY_TIME_PT IS NULL))
      AND (target.INGESTED_AT_PT IS NULL OR source.INGESTED_AT_PT > target.INGESTED_AT_PT)
    )
  )
THEN UPDATE SET
  %s
WHEN NOT MATCHED THEN INSERT (
  %s
) VALUES (
  %s
)`, table, strings.Join(expressions, ",\n    "), strings.Join(updates, ",\n  "), strings.Join(insertColumns, ",\n  "), strings.Join(insertValues, ",\n  "))
}

func sourceColumnExpression(column ColumnDefinition) string {
	path := "value:" + quoteIdent(column.Name)
	var expression string
	switch {
	case column.IsPayload:
		expression = path
	case column.IsDateTime:
		expression = "TRY_TO_TIMESTAMP_TZ(" + path + "::VARCHAR)"
	case column.SQLType == "DATE":
		expression = "TRY_TO_DATE(" + path + "::VARCHAR)"
	case column.SQLType == "FLOAT":
		expression = "TRY_TO_DOUBLE(" + path + "::VARCHAR)"
	case strings.HasPrefix(column.SQLType, "NUMBER"):
		expression = "TRY_TO_NUMBER(" + path + "::VARCHAR)"
	case column.SQLType == "BOOLEAN":
		expression = "TRY_TO_BOOLEAN(" + path + "::VARCHAR)"
	default:
		expression = path + "::VARCHAR"
	}
	return expression + " AS " + quoteIdent(column.Name)
}

func tradeTableDefinition() TradeTableDefinition {
	tradeTableDefinitionOnce.Do(func() {
		tradeTableDefinitionValue = buildTradeTableDefinition()
	})
	return tradeTableDefinitionValue
}

var (
	tradeTableDefinitionOnce  sync.Once
	tradeTableDefinitionValue TradeTableDefinition
)

func buildTradeTableDefinition() TradeTableDefinition {
	businessColumns := appendColumns(blockColumns(), allocationColumns()...)
	businessColumns = appendColumns(businessColumns, collectionColumns()...)
	columns := prioritizeColumns(businessColumns, consumerPriorityColumns())
	columns = appendColumns(columns, operationalColumns()...)
	columns = appendColumns(columns, technicalColumns()...)
	columnByName := make(map[string]ColumnDefinition, len(columns))
	for _, definition := range columns {
		columnByName[definition.Name] = definition
	}
	return TradeTableDefinition{Columns: columns, columnByName: columnByName}
}

func collectionColumns() []ColumnDefinition {
	columns := make([]ColumnDefinition, 0, len(repeatedFieldColumns()))
	for _, field := range repeatedFieldColumns() {
		columns = append(columns, columnWithComment(
			field.Column,
			"VARCHAR",
			"RFC 4180 comma-separated values in the original Aladdin array order.",
		))
	}
	return columns
}

func operationalColumns() []ColumnDefinition {
	return []ColumnDefinition{
		column("PORTFOLIO_FILTER_TYPE", "VARCHAR"), column("PORTFOLIO_FILTER_VALUE", "VARCHAR"), column("REFRESH_RUN_ID", "VARCHAR"),
		column("WATERMARK_START_PT", "TIMESTAMP_TZ", true), column("WATERMARK_END_PT", "TIMESTAMP_TZ", true),
		column("PAGE_NUMBER", "NUMBER(38,0)"), column("BLOCK_ORDINAL", "NUMBER(38,0)"), column("TRADE_ORDINAL", "NUMBER(38,0)"),
		column("INGESTED_AT_PT", "TIMESTAMP_TZ", true),
	}
}

func technicalColumns() []ColumnDefinition {
	return []ColumnDefinition{
		columnWithComment("BLOCK_CURRENT_KEY", "VARCHAR", "Deterministic key grouping allocation rows from the same Aladdin block trade."),
		columnWithComment("BLOCK_VERSION_KEY", "VARCHAR", "Deterministic hash of block-level source attributes, excluding allocations."),
		columnWithComment("TRADE_CURRENT_KEY", "VARCHAR", "Deterministic allocation-row identity used by the idempotent Snowflake MERGE."),
		columnWithComment("TRADE_VERSION_KEY", "VARCHAR", "Deterministic hash of the complete block and allocation source used for change detection."),
		payloadColumn("SOURCE_DATA", "VARIANT", "Complete normalized source block and allocation retained for audit and forward schema compatibility."),
		columnWithComment("TRADE_ID", "VARCHAR", "Aladdin trade identifier retained for source traceability."),
	}
}

func consumerPriorityColumns() []string {
	return []string{
		"AS_OF_DATE", "PORTFOLIO_NUMBER", "PORTFOLIO_ID", "INVNUM", "TRADE_NUMBER", "ORDER_ID", "MULTI_FUND_ID",
		"MODIFY_TIME_PT", "SOURCE_MODIFY_TIME_PT", "TRANSACTION_TYPE", "STATUS",
		"ASSET_REFERENCE_ASSET_ID", "ASSET_REFERENCE_CUSIP", "ASSET_REFERENCE_ISIN", "ASSET_REFERENCE_SEDOL", "ASSET_REFERENCE_BLOOMBERG_TICKER",
		"ASSET_EXPAND_REFERENCE_ASSET_ID", "ASSET_EXPAND_REFERENCE_CUSIP", "ASSET_EXPAND_REFERENCE_ISIN", "ASSET_EXPAND_REFERENCE_SEDOL", "ASSET_EXPAND_REFERENCE_BLOOMBERG_TICKER",
		"QUANTITY", "ALLOCATION_AMOUNT", "PRINCIPAL", "PRICE", "SETTLEMENT_DATE", "SETTLEMENT_CURRENCY",
		"BROKER_ID", "BROKER_TICKER", "EXECUTING_BROKER_ID", "EXECUTING_BROKER_TICKER",
	}
}

func prioritizeColumns(columns []ColumnDefinition, priority []string) []ColumnDefinition {
	byName := make(map[string]ColumnDefinition, len(columns))
	for _, column := range columns {
		byName[column.Name] = column
	}
	ordered := make([]ColumnDefinition, 0, len(columns))
	for _, name := range priority {
		if column, ok := byName[name]; ok {
			ordered = append(ordered, column)
			delete(byName, name)
		}
	}
	for _, column := range columns {
		if _, ok := byName[column.Name]; ok {
			ordered = append(ordered, column)
			delete(byName, column.Name)
		}
	}
	return ordered
}

func blockColumns() []ColumnDefinition {
	columns := []ColumnDefinition{
		column("TRADE_COUNT", "NUMBER(38,0)"), column("SOURCE_MODIFY_TIME_PT", "TIMESTAMP_TZ", true),
		column("ACCOUNTING_DESIGNATION", "VARCHAR"), column("ACTUAL_SETTLEMENT_DATE", "DATE"), column("AUTHORIZED_TIME_PT", "TIMESTAMP_TZ", true),
		column("BROKER_DESK_ID", "VARCHAR"), column("BROKER_REASON", "VARCHAR"),
	}
	columns = append(columns, assetReferenceColumns("ASSET_REFERENCE")...)
	columns = append(columns, assetReferenceColumns("ASSET_EXPAND_REFERENCE")...)
	columns = append(columns, assetReferenceColumns("ASSET_SUMMARY_REFERENCE")...)
	columns = append(columns,
		column("ASSET_SUMMARY_ISSUE_COUNTRY_CODE", "VARCHAR"), column("ASSET_SUMMARY_SECURITY_CURRENCY", "VARCHAR"), column("ASSET_SUMMARY_SECURITY_GROUP", "VARCHAR"), column("ASSET_SUMMARY_SECURITY_TYPE", "VARCHAR"),
	)
	columns = append(columns, brokerReferenceColumns("BROKER")...)
	columns = append(columns,
		column("COLLATERAL_EXPOSURE_TYPE", "VARCHAR"), column("CONFIRMED_BY", "VARCHAR"), column("CONFIRMED_WITH", "VARCHAR"), column("CONVEXITY", "FLOAT"),
		column("COUPON", "FLOAT"), column("COUPON_TYPE", "VARCHAR"), column("DAYS_TO_MATURITY_2A7", "NUMBER(10,0)"), column("DEALING_CAPACITY", "VARCHAR"),
		column("DROP_RATE", "FLOAT"), column("DURATION", "FLOAT"), column("EFFECTIVE_RATE", "FLOAT"), column("ENTERED_BY", "VARCHAR"), column("ENTRY_TIME_PT", "TIMESTAMP_TZ", true),
		column("EXECUTING_BROKER_DESK_ID", "VARCHAR"),
	)
	columns = append(columns, brokerReferenceColumns("EXECUTING_BROKER")...)
	columns = append(columns,
		column("EXECUTION_TIME_PT", "TIMESTAMP_TZ", true), column("EXECUTION_TIME_SOURCE", "VARCHAR"), column("FACTOR", "FLOAT"), column("FACTOR_DATE", "DATE"),
	)
	columns = append(columns, assetReferenceColumns("FORWARD_ASSET_REFERENCE")...)
	columns = append(columns,
		column("FORWARD_POINTS", "FLOAT"), column("FORWARD_PRICE", "FLOAT"), column("FX_SPOT_PRICE", "FLOAT"), column("MULTI_FUND_ID", "NUMBER(10,0)"),
		column("PERCENT_YIELD", "FLOAT"), column("PLACEMENT_ID", "VARCHAR"), column("POOL_STIPULATIONS", "VARCHAR"), column("PREPAY_TYPE", "VARCHAR"),
		column("PRICE", "FLOAT"), column("PRICE_TYPE", "VARCHAR"), column("PRICING_INDEX", "VARCHAR"), column("PRICING_SPREAD", "FLOAT"), column("PSA", "FLOAT"),
		column("PURPOSE", "VARCHAR"), column("REINVESTMENT_RATE", "FLOAT"), column("REVIEW_TIME_PT", "TIMESTAMP_TZ", true), column("SETTLEMENT_CURRENCY", "VARCHAR"),
		column("SETTLEMENT_CURRENCY_PAIR", "VARCHAR"), column("SETTLEMENT_DATE", "DATE"), column("SETTLEMENT_FX_RATE", "FLOAT"), column("STATUS", "VARCHAR"),
		column("SUPPRESS_EXTERNAL_NOTIFICATION", "BOOLEAN"), column("SWAP_POINTS", "FLOAT"), column("TBA_STIPULATIONS", "VARCHAR"), column("BLOCK_TOUCH_COUNT", "NUMBER(10,0)"),
		column("AS_OF_DATE", "DATE"), column("TRADE_DISCOUNT_PRICE", "FLOAT"), column("TRADE_YIELD_TO_CALL", "FLOAT"), column("TRADER_INITIALS", "VARCHAR"),
		column("TRANSACTION_TYPE", "VARCHAR"), column("TRANSFER_EXTERNAL_ACCOUNT_NAME", "VARCHAR"),
	)
	columns = append(columns, portfolioReferenceColumns("TRANSFER_PORTFOLIO")...)
	columns = append(columns, column("BLOCK_VERSION", "NUMBER(10,0)"), column("WEIGHTED_AVERAGE_COUPON", "FLOAT"), column("WEIGHTED_AVERAGE_MATURITY", "NUMBER(10,0)"))
	return columns
}

func allocationColumns() []ColumnDefinition {
	columns := []ColumnDefinition{
		column("ADDITIONAL_TRADE_CHARGE", "FLOAT"), column("ALLOCATION_AMOUNT", "FLOAT"), column("COMMISSION", "FLOAT"), column("CURRENT_FACTORED_DOWN_AMOUNT", "FLOAT"),
		column("EFFECTIVE_TERM_DATE", "DATE"), column("EXCHANGE", "VARCHAR"), column("EXCHANGE_RATE", "FLOAT"), column("FEE", "FLOAT"),
		column("FORWARD_INTEREST", "FLOAT"), column("FORWARD_PRINCIPAL", "FLOAT"), column("FORWARD_QUANTITY", "FLOAT"), column("FORWARD_SETTLEMENT_INSTRUCTION_ID1", "VARCHAR"),
		column("FX_FIRST_LEG_CURRENCY_CODE", "VARCHAR"), column("FX_FIRST_LEG_SETTLEMENT_INSTRUCTION", "NUMBER(10,0)"),
		column("FX_SECOND_LEG_CURRENCY_CODE", "VARCHAR"), column("FX_SECOND_LEG_SETTLEMENT_INSTRUCTION", "NUMBER(10,0)"),
		column("INTEREST", "FLOAT"), column("INTEREST_AT_MATURITY", "FLOAT"), column("INVNUM", "NUMBER(10,0)"), column("MODIFY_TIME_PT", "TIMESTAMP_TZ", true),
		column("MORTGAGE_BACKED_SECURITIES_CLEARING_CORPORATION_TYPE", "VARCHAR"), column("ORDER_ID", "VARCHAR"),
	}
	columns = append(columns, column("PORTFOLIO_ID", "VARCHAR"), column("PORTFOLIO_NUMBER", "VARCHAR"))
	columns = append(columns,
		column("PRINCIPAL", "FLOAT"), column("QUANTITY", "FLOAT"), column("ROLL_FEE", "FLOAT"), column("SERIES_NUMBER", "NUMBER(10,0)"), column("SETTLEMENT_INSTRUCTION_ID1", "VARCHAR"),
		column("STRATEGY_ID", "VARCHAR"), column("STRATEGY_NAME", "VARCHAR"),
		column("FLAG_ALLOW_DIRTY_PRICE", "BOOLEAN"), column("FLAG_BENEFICIAL_OWNERSHIP_CHANGE", "BOOLEAN"), column("FLAG_CASH_HAIRCUT", "BOOLEAN"),
		column("FLAG_COMPLIANCE_PENDING", "BOOLEAN"), column("FLAG_DELIVERY_FREE_PAYMENT", "BOOLEAN"), column("FLAG_ELECTRONIC_POOL_NOTIFICATION_ELIGIBLE", "BOOLEAN"),
		column("FLAG_FULLY_ALLOCATED", "BOOLEAN"), column("FLAG_NETTED", "BOOLEAN"), column("FLAG_OUTSIDE_COMMITMENT", "BOOLEAN"), column("FLAG_SUPPRESS_CONTRACT", "BOOLEAN"),
		column("TRADE_NUMBER", "NUMBER(10,0)"),
	)
	return columns
}

func assetReferenceColumns(prefix string) []ColumnDefinition {
	return []ColumnDefinition{
		column(prefix+"_ASSET_ID", "VARCHAR"), column(prefix+"_BLOOMBERG_TICKER", "VARCHAR"), column(prefix+"_CINS", "VARCHAR"),
		column(prefix+"_CUSIP", "VARCHAR"), column(prefix+"_ISIN", "VARCHAR"), column(prefix+"_SEDOL", "VARCHAR"),
	}
}

func brokerReferenceColumns(prefix string) []ColumnDefinition {
	return []ColumnDefinition{column(prefix+"_ID", "VARCHAR"), column(prefix+"_SHORTNAME", "VARCHAR"), column(prefix+"_TICKER", "VARCHAR")}
}

func portfolioReferenceColumns(prefix string) []ColumnDefinition {
	return []ColumnDefinition{column(prefix+"_ID", "VARCHAR"), column(prefix+"_TICKER", "VARCHAR")}
}

func appendColumns(columns []ColumnDefinition, extra ...ColumnDefinition) []ColumnDefinition {
	return append(columns, extra...)
}

func column(name string, sqlType string, options ...bool) ColumnDefinition {
	definition := ColumnDefinition{Name: name, SQLType: sqlType}
	if len(options) > 0 {
		definition.IsDateTime = options[0]
	}
	return definition
}

func columnWithComment(name, sqlType, comment string) ColumnDefinition {
	definition := column(name, sqlType)
	definition.Comment = comment
	return definition
}

func payloadColumn(name, sqlType string, comments ...string) ColumnDefinition {
	definition := ColumnDefinition{Name: name, SQLType: sqlType, IsPayload: true}
	if len(comments) > 0 {
		definition.Comment = comments[0]
	}
	return definition
}
