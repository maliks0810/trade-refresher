package services

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSnowflakeSchemaUsesTwoDurableTables(t *testing.T) {
	definition := tradeTableDefinition()
	if got, want := len(definition.Columns), 216; got != want {
		t.Fatalf("TRADES column count = %d, want documented count %d", got, want)
	}
	seen := map[string]bool{}
	for _, column := range definition.Columns {
		if seen[column.Name] {
			t.Fatalf("duplicate TRADES column %s", column.Name)
		}
		seen[column.Name] = true
	}
	for _, required := range []string{
		"TRADE_CURRENT_KEY", "TRADE_VERSION_KEY", "TRADE_ID", "PORTFOLIO_NUMBER",
		"MODIFY_TIME_PT", "AS_OF_DATE", "FLAG_FULLY_ALLOCATED", "FX_FIRST_LEG_CURRENCY_CODE",
		"TRADE_COMMENTS_COMMENT", "COLLATERALS_CASH_PERCENTAGE",
		"TRADE_RELATIONSHIPS_APPLY_TIME_PT", "SOURCE_DATA",
	} {
		if !seen[required] {
			t.Fatalf("TRADES definition is missing %s", required)
		}
	}
	wantFirst := []string{"AS_OF_DATE", "PORTFOLIO_NUMBER"}
	for index, name := range wantFirst {
		if definition.Columns[index].Name != name {
			t.Fatalf("TRADES column %d = %s, want consumer-first %s", index+1, definition.Columns[index].Name, name)
		}
	}
	wantLast := []string{"BLOCK_CURRENT_KEY", "BLOCK_VERSION_KEY", "TRADE_CURRENT_KEY", "TRADE_VERSION_KEY", "SOURCE_DATA", "TRADE_ID"}
	for index, name := range wantLast {
		column := definition.Columns[len(definition.Columns)-len(wantLast)+index]
		if column.Name != name {
			t.Fatalf("TRADES technical column %d = %s, want %s", index+1, column.Name, name)
		}
		if column.Comment == "" {
			t.Fatalf("TRADES technical column %s is missing a Snowflake comment", name)
		}
	}
	for _, column := range definition.Columns {
		if strings.HasPrefix(column.Name, "ALLOCATION_") && (column.Name == "ALLOCATION_VERSION" || column.Name == "ALLOCATION_TOUCH_COUNT") {
			t.Fatalf("undocumented OpenAPI column %s must not be exposed", column.Name)
		}
	}
	tradeDDL := createTradeTableSQL(`"TCW_CORE_DEV"."STAGING_ALADDIN"."TRADES"`)
	stateDDL := createStateTableSQL(`"TCW_CORE_DEV"."STAGING_ALADDIN"."TRADE_REFRESH_STATE"`)
	if !strings.Contains(tradeDDL, "CREATE TABLE IF NOT EXISTS") || !strings.Contains(stateDDL, "CREATE TABLE IF NOT EXISTS") {
		t.Fatal("startup DDL must be idempotent")
	}
	if strings.Contains(tradeDDL, "_STAGE") || strings.Contains(stateDDL, "_STAGE") {
		t.Fatal("durable stage tables must not be created")
	}
	if strings.Contains(tradeDDL, "TIMESTAMP_NTZ") || strings.Contains(stateDDL, "TIMESTAMP_NTZ") || !strings.Contains(tradeDDL, "TIMESTAMP_TZ") {
		t.Fatal("Pacific timestamps must preserve their PST/PDT offset with TIMESTAMP_TZ")
	}
	if !strings.Contains(tradeDDL, `"TRADE_COMMENTS_COMMENT" VARCHAR COMMENT 'RFC 4180`) || !strings.Contains(tradeDDL, `"SOURCE_DATA" VARIANT`) || !strings.Contains(tradeDDL, "COMMENT 'Deterministic") {
		t.Fatal("TRADES DDL must expose repeated fields as CSV text and document technical columns")
	}
	for _, column := range []string{
		"PORTFOLIO_FILTER_KEY", "PORTFOLIO_GROUP_TICKER", "PORTFOLIO_GROUP_MEMBERS", "PORTFOLIO_GROUP_FETCHED_AT_PT",
		"RECOVERY_PORTFOLIO_ID", "RECOVERY_PORTFOLIO_TICKER", "RECOVERY_START_PT", "RECOVERY_END_PT",
		"RECOVERY_NEXT_ATTEMPT_PT", "RECOVERY_ATTEMPTS", "RECOVERY_LAST_ERROR",
	} {
		if !strings.Contains(stateDDL, column) {
			t.Fatalf("state table is missing durable membership column %s", column)
		}
	}
	if got := len(stateTableMigrationSQL(`"TCW_CORE_DEV"."STAGING_ALADDIN"."TRADE_REFRESH_STATE"`)); got != 11 {
		t.Fatalf("state migration statement count = %d, want 11", got)
	}
	merge := mergeTradesSQL(`"TCW_CORE_DEV"."STAGING_ALADDIN"."TRADES"`, definition)
	if !strings.Contains(merge, "THEN UPDATE SET") || !strings.Contains(merge, "WHEN NOT MATCHED THEN INSERT") {
		t.Fatal("TRADES write must use one explicit set-based MERGE")
	}
	if strings.Contains(merge, "ALL BY NAME") {
		t.Fatal("MERGE must tolerate unrelated legacy target columns")
	}
	if !strings.Contains(merge, "source.MODIFY_TIME_PT > target.MODIFY_TIME_PT") || !strings.Contains(merge, "source.INGESTED_AT_PT > target.INGESTED_AT_PT") {
		t.Fatal("MERGE must reject stale trade versions during page replay")
	}
	if strings.Contains(merge, "source.TRADE_VERSION_KEY > target.TRADE_VERSION_KEY") {
		t.Fatal("a content hash must not be used as a recency sequence")
	}
	if strings.Contains(merge, "ALLOCATION_VERSION") || strings.Contains(merge, "ALLOCATION_TOUCH_COUNT") {
		t.Fatal("MERGE must not depend on undocumented allocation version fields")
	}
}

func TestSnowflakeSessionAndLeaseUsePacificTime(t *testing.T) {
	parameters := snowflakeSessionParameters()
	timezone, ok := parameters["timezone"]
	if !ok || timezone == nil || *timezone != "America/Los_Angeles" {
		t.Fatalf("Snowflake timezone parameter = %#v", timezone)
	}
	if got, want := snowflakePacificTimestampAfterSeconds(), "CONVERT_TIMEZONE('America/Los_Angeles', DATEADD(second, ?, CURRENT_TIMESTAMP()))"; got != want {
		t.Fatalf("lease timestamp expression = %q, want %q", got, want)
	}
}

func TestSnowflakeBatchKeepsVariantNativeAndConvertsPacificTime(t *testing.T) {
	rows := []TradeTableRow{{Values: map[string]any{
		"TRADE_CURRENT_KEY": "key-1",
		"TRADE_VERSION_KEY": "version-1",
		"MODIFY_TIME_PT":    time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC),
		"SOURCE_DATA":       `{"block":{"status":"C"},"trade":{"id":"trade-1"}}`,
	}}}
	end, payload, err := marshalTradeBatch(rows, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if end != 1 {
		t.Fatalf("batch end = %d, want 1", end)
	}
	var decoded []map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded[0]["SOURCE_DATA"].(map[string]any); !ok {
		t.Fatalf("SOURCE_DATA should be nested JSON, got %T", decoded[0]["SOURCE_DATA"])
	}
	if _, ok := decoded[0]["PRICE"]; ok {
		t.Fatal("absent fields should be omitted from the Snowflake JSON payload")
	}
	if got, want := decoded[0]["MODIFY_TIME_PT"], "2026-07-08T09:00:00-07:00"; got != want {
		t.Fatalf("MODIFY_TIME_PT = %v, want %s", got, want)
	}
}

func TestSnowflakeBatchStopsBeforeByteLimit(t *testing.T) {
	largeValue := strings.Repeat("x", 3<<20)
	rows := make([]TradeTableRow, 3)
	for index := range rows {
		rows[index] = TradeTableRow{Values: map[string]any{
			"TRADE_CURRENT_KEY": "key-" + string(rune('1'+index)),
			"TRADE_VERSION_KEY": "version-1",
			"SOURCE_DATA":       `{"value":"` + largeValue + `"}`,
		}}
	}
	end, payload, err := marshalTradeBatch(rows, 0, len(rows))
	if err != nil {
		t.Fatal(err)
	}
	if end != 2 {
		t.Fatalf("batch end = %d, want 2", end)
	}
	if len(payload) > maxSnowflakeJSONBatchBytes {
		t.Fatalf("batch bytes = %d, limit = %d", len(payload), maxSnowflakeJSONBatchBytes)
	}
}

func BenchmarkMarshalTradeBatch(b *testing.B) {
	rows := make([]TradeTableRow, 1000)
	for index := range rows {
		id := strconv.Itoa(index)
		rows[index] = TradeTableRow{Values: map[string]any{
			"TRADE_CURRENT_KEY": "trade-" + id,
			"TRADE_VERSION_KEY": "version-1",
			"TRADE_ID":          id,
			"PORTFOLIO_NUMBER":  "702T",
			"MODIFY_TIME_PT":    time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC),
			"SOURCE_DATA":       `{"trade":{"id":"` + id + `"}}`,
		}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		end, payload, err := marshalTradeBatch(rows, 0, len(rows))
		if err != nil || end != len(rows) || len(payload) == 0 {
			b.Fatalf("marshalTradeBatch end=%d bytes=%d err=%v", end, len(payload), err)
		}
	}
}

func TestTradeExtractorCoversOpenAPITopLevelProperties(t *testing.T) {
	spec := loadOpenAPIDocument(t)
	requireSchemaCovered(t, spec, "tradev2BlockTrade", coveredProperties(blockFieldSpecs,
		"asset", "assetReference", "brokerReference", "executingBrokerReference", "forwardAssetReference",
		"tradeComments", "tradeQuotes", "trades", "transferPortfolioReference", "userDefinedFields",
	))
	requireSchemaCovered(t, spec, "tradev2Trade", coveredProperties(tradeFieldSpecs,
		"collaterals", "contracts", "externalTradeReferences", "forwardExternalTradeReferences", "fxLegs",
		"origOrderReferences", "portfolioReference", "strategyReference", "tradeCharges", "tradeFlags", "tradeRelationships",
	))
	requireSchemaCovered(t, spec, "tradev2AssetSummary", coveredProperties(assetSummaryFieldSpecs, "reference"))
	requireSchemaCovered(t, spec, "tradev2TradeFlags", stringSet(
		"allowDirtyPrice", "beneficialOwnershipChange", "cashHaircut", "compliancePending", "deliveryFreePayment",
		"electronicPoolNotificationEligible", "fullyAllocated", "netted", "outsideCommitment", "suppressContract",
	))
	requireSchemaCovered(t, spec, "v2TradeFXLegs", stringSet("firstLeg", "secondLeg"))
	requireSchemaCovered(t, spec, "TradeFXLegsFXLeg", stringSet("currencyCode", "settlementInstruction"))
	requireSchemaCovered(t, spec, "tradev2AssetExpand", stringSet("reference", "summary"))
	requireSchemaCovered(t, spec, "tradev2TradeCollateral", coveredProperties(collateralFieldSpecs, "assetReference"))
	requireSchemaCovered(t, spec, "tradev2Contract", coveredProperties(contractFieldSpecs, "assetReference"))
	requireSchemaCovered(t, spec, "tradev2ExternalTradeReference", coveredProperties(externalReferenceFieldSpecs))
	requireSchemaCovered(t, spec, "v2OriginalOrderReference", map[string]bool{"externalOrderReferences": true, "originalOrderId": true})
	requireSchemaCovered(t, spec, "tradingorder_managementorderv1ExternalOrderReference", coveredProperties(originalOrderExternalFieldSpecs))
	requireSchemaCovered(t, spec, "tradev2TradeCharge", coveredProperties(chargeFieldSpecs))
	requireSchemaCovered(t, spec, "tradev2TradeComment", coveredProperties(commentFieldSpecs))
	requireSchemaCovered(t, spec, "tradev2TradeQuote", coveredProperties(quoteFieldSpecs))
	requireSchemaCovered(t, spec, "tradev2TradeRelationship", coveredProperties(relationshipFieldSpecs, "portfolioReference1", "portfolioReference2"))
	requireSchemaCovered(t, spec, "tradev2UserDefinedField", coveredProperties(userDefinedFieldSpecs))
	for _, prefix := range []string{"ASSET_REFERENCE", "ASSET_EXPAND_REFERENCE", "ASSET_SUMMARY_REFERENCE", "FORWARD_ASSET_REFERENCE"} {
		requireReferenceColumns(t, spec, "referencesassetAssetReference", assetReferenceColumns(prefix), prefix, "")
	}
	for _, prefix := range []string{"BROKER", "EXECUTING_BROKER"} {
		requireReferenceColumns(t, spec, "referencesbrokerBrokerReference", brokerReferenceColumns(prefix), prefix, "BROKER")
	}
	requireReferenceColumns(t, spec, "agraphcommoncommonreferencesportfolioPortfolioReference", portfolioReferenceColumns("TRANSFER_PORTFOLIO"), "TRANSFER_PORTFOLIO", "PORTFOLIO")
	requireAliasedReferenceColumns(t, spec, "agraphcommoncommonreferencesportfolioPortfolioReference", map[string]string{
		"portfolioId": "PORTFOLIO_ID", "portfolioTicker": "PORTFOLIO_NUMBER",
	})
	requireRepeatedReferenceColumns(t, spec, "referencesassetAssetReference", "COLLATERALS_ASSET_REFERENCE")
	requireRepeatedReferenceColumns(t, spec, "referencesassetAssetReference", "CONTRACTS_ASSET_REFERENCE")
	requireRepeatedReferenceColumns(t, spec, "agraphcommoncommonreferencesportfolioPortfolioReference", "TRADE_RELATIONSHIPS_PORTFOLIO_1")
	requireRepeatedReferenceColumns(t, spec, "agraphcommoncommonreferencesportfolioPortfolioReference", "TRADE_RELATIONSHIPS_PORTFOLIO_2")
	requireReferenceColumns(t, spec, "referencesstrategyStrategyReference", []ColumnDefinition{column("STRATEGY_ID", "VARCHAR"), column("STRATEGY_NAME", "VARCHAR")}, "STRATEGY", "STRATEGY")
}

func TestTradeExtractorScalarTypesMatchOpenAPI(t *testing.T) {
	spec := loadOpenAPIDocument(t)
	checks := []struct {
		schema string
		prefix string
		specs  []fieldSpec
		array  bool
	}{
		{schema: "tradev2BlockTrade", specs: blockFieldSpecs},
		{schema: "tradev2Trade", specs: tradeFieldSpecs},
		{schema: "tradev2AssetSummary", specs: assetSummaryFieldSpecs},
		{schema: "tradev2TradeComment", prefix: "TRADE_COMMENTS", specs: commentFieldSpecs, array: true},
		{schema: "tradev2TradeQuote", prefix: "TRADE_QUOTES", specs: quoteFieldSpecs, array: true},
		{schema: "tradev2UserDefinedField", prefix: "USER_DEFINED_FIELDS", specs: userDefinedFieldSpecs, array: true},
		{schema: "tradev2TradeCollateral", prefix: "COLLATERALS", specs: collateralFieldSpecs, array: true},
		{schema: "tradev2Contract", prefix: "CONTRACTS", specs: contractFieldSpecs, array: true},
		{schema: "tradev2ExternalTradeReference", prefix: "EXTERNAL_TRADE_REFERENCES", specs: externalReferenceFieldSpecs, array: true},
		{schema: "tradev2ExternalTradeReference", prefix: "FORWARD_EXTERNAL_TRADE_REFERENCES", specs: externalReferenceFieldSpecs, array: true},
		{schema: "tradev2TradeCharge", prefix: "TRADE_CHARGES", specs: chargeFieldSpecs, array: true},
		{schema: "tradev2TradeRelationship", prefix: "TRADE_RELATIONSHIPS", specs: relationshipFieldSpecs, array: true},
		{schema: "v2OriginalOrderReference", prefix: "ORIGINAL_ORDER_REFERENCES", specs: []fieldSpec{field("ORIGINAL_ORDER_ID", kindString, "originalOrderId")}, array: true},
		{schema: "tradingorder_managementorderv1ExternalOrderReference", prefix: "ORIGINAL_ORDER_REFERENCES", specs: []fieldSpec{
			field("EXTERNAL_ACCOUNT_CODE", kindString, "accountCode"),
			field("EXTERNAL_ID1", kindString, "externalId1"),
			field("EXTERNAL_ID2", kindString, "externalId2"),
		}, array: true},
		{schema: "TradeFXLegsFXLeg", specs: []fieldSpec{
			field("FX_FIRST_LEG_CURRENCY_CODE", kindString, "currencyCode"),
			field("FX_FIRST_LEG_SETTLEMENT_INSTRUCTION", kindInteger, "settlementInstruction"),
			field("FX_SECOND_LEG_CURRENCY_CODE", kindString, "currencyCode"),
			field("FX_SECOND_LEG_SETTLEMENT_INSTRUCTION", kindInteger, "settlementInstruction"),
		}},
		{schema: "tradev2TradeFlags", specs: []fieldSpec{
			field("FLAG_ALLOW_DIRTY_PRICE", kindBoolean, "allowDirtyPrice"),
			field("FLAG_BENEFICIAL_OWNERSHIP_CHANGE", kindBoolean, "beneficialOwnershipChange"),
			field("FLAG_CASH_HAIRCUT", kindBoolean, "cashHaircut"),
			field("FLAG_COMPLIANCE_PENDING", kindBoolean, "compliancePending"),
			field("FLAG_DELIVERY_FREE_PAYMENT", kindBoolean, "deliveryFreePayment"),
			field("FLAG_ELECTRONIC_POOL_NOTIFICATION_ELIGIBLE", kindBoolean, "electronicPoolNotificationEligible"),
			field("FLAG_FULLY_ALLOCATED", kindBoolean, "fullyAllocated"),
			field("FLAG_NETTED", kindBoolean, "netted"),
			field("FLAG_OUTSIDE_COMMITMENT", kindBoolean, "outsideCommitment"),
			field("FLAG_SUPPRESS_CONTRACT", kindBoolean, "suppressContract"),
		}},
	}
	for _, check := range checks {
		requireFieldTypesMatchOpenAPI(t, spec, check.schema, check.prefix, check.specs, check.array)
	}
}

func stringSet(values ...string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

type openAPIDocument struct {
	Paths      map[string]map[string]openAPIOperation `json:"paths"`
	Components struct {
		Schemas map[string]openAPISchema `json:"schemas"`
	} `json:"components"`
}

type openAPIOperation struct {
	OperationID string `json:"operationId"`
	AccessType  string `json:"x-access-type"`
	QuotaLimit  int    `json:"x-quota-limit"`
}

func TestTradeOperationDefinitionsMatchOpenAPI(t *testing.T) {
	spec := loadOpenAPIDocument(t)
	expected := make(map[string]TradeOperation)
	for _, operation := range []TradeOperation{
		OperationGetLongRunningOperation,
		OperationFilterTrades,
		OperationRetrieveTrades,
		OperationPostTrade,
		OperationBatchPostTrades,
		OperationCancelTrade,
		OperationBatchCancelTrades,
	} {
		expected[strings.ToLower(operation.Method)+" "+operation.UpstreamPath] = operation
	}

	actualCount := 0
	for path, methods := range spec.Paths {
		for method, operation := range methods {
			actualCount++
			key := strings.ToLower(method) + " " + path
			definition, ok := expected[key]
			if !ok {
				t.Fatalf("OpenAPI operation %s is not implemented", key)
			}
			if definition.Name != operation.OperationID || !strings.EqualFold(string(definition.QuotaFamily), operation.AccessType) || definition.QuotaLimit != operation.QuotaLimit {
				t.Fatalf("operation %s differs from OpenAPI: %#v vs %#v", key, definition, operation)
			}
		}
	}
	if actualCount != len(expected) {
		t.Fatalf("implemented operation count = %d, OpenAPI count = %d", len(expected), actualCount)
	}
}

type openAPISchema struct {
	Properties map[string]json.RawMessage `json:"properties"`
	Enum       []string                   `json:"enum"`
	Type       string                     `json:"type"`
	Format     string                     `json:"format"`
}

func TestFilterAndRetrieveResponseContractsMatch(t *testing.T) {
	spec := loadOpenAPIDocument(t)
	filter := spec.Components.Schemas["tradev2FilterTradesResponse"]
	retrieve := spec.Components.Schemas["tradev2RetrieveTradesResponse"]

	itemReference := func(schema openAPISchema) string {
		t.Helper()
		var property struct {
			Items struct {
				Reference string `json:"$ref"`
			} `json:"items"`
		}
		if err := json.Unmarshal(schema.Properties["blockTrades"], &property); err != nil {
			t.Fatal(err)
		}
		return property.Items.Reference
	}

	const blockTradeReference = "#/components/schemas/tradev2BlockTrade"
	if got := itemReference(filter); got != blockTradeReference {
		t.Fatalf("filter blockTrades item = %q, want %q", got, blockTradeReference)
	}
	if got := itemReference(retrieve); got != blockTradeReference {
		t.Fatalf("retrieve blockTrades item = %q, want %q", got, blockTradeReference)
	}
	if _, ok := filter.Properties["nextPageToken"]; !ok {
		t.Fatal("filter response no longer exposes nextPageToken")
	}
	if _, ok := retrieve.Properties["nextPageToken"]; ok {
		t.Fatal("retrieve response unexpectedly exposes nextPageToken")
	}
}

func TestRefreshRequestsEveryOpenAPITradeEnrichment(t *testing.T) {
	spec := loadOpenAPIDocument(t)
	schema, ok := spec.Components.Schemas["v2FilterQueryOption"]
	if !ok {
		t.Fatal("OpenAPI v2FilterQueryOption schema not found")
	}
	expected := map[string]bool{}
	for _, option := range schema.Enum {
		if !strings.HasSuffix(option, "_UNSPECIFIED") {
			expected[option] = true
		}
	}
	scheduler := &TradeScheduler{cfg: SchedulerConfig{
		PageSize:        1000,
		PortfolioFilter: PortfolioFilter{Type: PortfolioFilterGroup, Values: []string{"TCW_ALL"}},
	}}
	body, err := scheduler.filterRequestBody(time.Now().Add(-time.Minute), time.Now(), "")
	if err != nil {
		t.Fatal(err)
	}
	var request struct {
		Expands []string `json:"expands"`
		Query   struct {
			Options []string `json:"options"`
		} `json:"query"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	if len(request.Query.Options) != len(expected) {
		t.Fatalf("refresh options = %v, OpenAPI enrichments = %v", request.Query.Options, expected)
	}
	for _, option := range request.Query.Options {
		if !expected[option] {
			t.Fatalf("refresh option %s is not an OpenAPI enrichment", option)
		}
	}
	if len(request.Expands) != 1 || request.Expands[0] != "asset.summary" {
		t.Fatalf("refresh expands = %v, want asset.summary", request.Expands)
	}
}

func loadOpenAPIDocument(t *testing.T) openAPIDocument {
	t.Helper()
	contents, err := os.ReadFile("../../../openapi.json")
	if err != nil {
		t.Fatalf("read OpenAPI spec: %v", err)
	}
	var spec openAPIDocument
	if err := json.Unmarshal(contents, &spec); err != nil {
		t.Fatalf("parse OpenAPI spec: %v", err)
	}
	return spec
}

func coveredProperties(specs []fieldSpec, extra ...string) map[string]bool {
	covered := map[string]bool{}
	for _, spec := range specs {
		if len(spec.Path) > 0 {
			covered[spec.Path[0]] = true
		}
	}
	for _, property := range extra {
		covered[property] = true
	}
	return covered
}

func requireSchemaCovered(t *testing.T, spec openAPIDocument, schemaName string, covered map[string]bool) {
	t.Helper()
	schema, ok := spec.Components.Schemas[schemaName]
	if !ok {
		t.Fatalf("OpenAPI schema %s not found", schemaName)
	}
	var missing []string
	for property := range schema.Properties {
		if !covered[property] {
			missing = append(missing, property)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("OpenAPI schema %s has unmapped properties: %s", schemaName, strings.Join(missing, ", "))
	}
}

func requireReferenceColumns(t *testing.T, spec openAPIDocument, schemaName string, columns []ColumnDefinition, prefix, redundantPrefix string) {
	t.Helper()
	schema, ok := spec.Components.Schemas[schemaName]
	if !ok {
		t.Fatalf("OpenAPI schema %s not found", schemaName)
	}
	columnNames := map[string]bool{}
	for _, column := range columns {
		columnNames[column.Name] = true
	}
	for property := range schema.Properties {
		suffix := toSnake(property)
		if redundantPrefix != "" {
			suffix = strings.TrimPrefix(suffix, redundantPrefix+"_")
		}
		expected := prefix + "_" + suffix
		if !columnNames[expected] {
			t.Fatalf("OpenAPI schema %s property %s is missing reference column %s", schemaName, property, expected)
		}
		if column := tradeTableDefinition().columnByName[expected]; column.SQLType != "VARCHAR" {
			t.Fatalf("OpenAPI reference %s.%s column %s type = %s, want VARCHAR", schemaName, property, expected, column.SQLType)
		}
	}
}

func requireAliasedReferenceColumns(t *testing.T, spec openAPIDocument, schemaName string, aliases map[string]string) {
	t.Helper()
	schema, ok := spec.Components.Schemas[schemaName]
	if !ok {
		t.Fatalf("OpenAPI schema %s not found", schemaName)
	}
	for property := range schema.Properties {
		name, ok := aliases[property]
		if !ok {
			t.Fatalf("OpenAPI reference %s.%s has no Snowflake alias", schemaName, property)
		}
		column, ok := tradeTableDefinition().columnByName[name]
		if !ok || column.SQLType != "VARCHAR" {
			t.Fatalf("OpenAPI reference %s.%s column %s must be VARCHAR", schemaName, property, name)
		}
	}
}

func requireRepeatedReferenceColumns(t *testing.T, spec openAPIDocument, schemaName, prefix string) {
	t.Helper()
	schema, ok := spec.Components.Schemas[schemaName]
	if !ok {
		t.Fatalf("OpenAPI schema %s not found", schemaName)
	}
	for property := range schema.Properties {
		name := prefix + "_" + toSnake(property)
		column, ok := tradeTableDefinition().columnByName[name]
		if !ok {
			t.Fatalf("OpenAPI repeated reference %s.%s is missing column %s", schemaName, property, name)
		}
		if column.SQLType != "VARCHAR" {
			t.Fatalf("OpenAPI repeated reference %s.%s column %s must be VARCHAR CSV", schemaName, property, name)
		}
	}
}

func requireFieldTypesMatchOpenAPI(t *testing.T, spec openAPIDocument, schemaName, prefix string, specs []fieldSpec, array bool) {
	t.Helper()
	schema, ok := spec.Components.Schemas[schemaName]
	if !ok {
		t.Fatalf("OpenAPI schema %s not found", schemaName)
	}
	definition := tradeTableDefinition()
	for _, field := range specs {
		propertyName := field.Path[0]
		property, ok := schema.Properties[propertyName]
		if !ok {
			t.Fatalf("OpenAPI schema %s does not contain mapped property %s", schemaName, propertyName)
		}
		kind, err := openAPIPropertyKind(spec, property)
		if err != nil {
			t.Fatalf("OpenAPI schema %s property %s: %v", schemaName, propertyName, err)
		}
		if kind != field.Kind {
			t.Fatalf("OpenAPI schema %s property %s maps to kind %d, extractor uses %d", schemaName, propertyName, kind, field.Kind)
		}
		columnName := field.Column
		if prefix != "" {
			columnName = prefix + "_" + columnName
		}
		column, ok := definition.columnByName[columnName]
		if !ok {
			t.Fatalf("OpenAPI schema %s property %s is missing Snowflake column %s", schemaName, propertyName, columnName)
		}
		if array {
			if column.SQLType != "VARCHAR" {
				t.Fatalf("repeated OpenAPI property %s.%s column %s must be VARCHAR CSV", schemaName, propertyName, columnName)
			}
			continue
		}
		if want := snowflakeTypeForKind(kind); column.SQLType != want {
			t.Fatalf("OpenAPI schema %s property %s column %s type = %s, want %s", schemaName, propertyName, columnName, column.SQLType, want)
		}
	}
}

func openAPIPropertyKind(spec openAPIDocument, raw json.RawMessage) (scalarKind, error) {
	var property struct {
		Type      string `json:"type"`
		Format    string `json:"format"`
		Reference string `json:"$ref"`
	}
	if err := json.Unmarshal(raw, &property); err != nil {
		return 0, err
	}
	if property.Reference != "" {
		name := strings.TrimPrefix(property.Reference, "#/components/schemas/")
		schema, ok := spec.Components.Schemas[name]
		if !ok {
			return 0, fmt.Errorf("referenced schema %s not found", name)
		}
		property.Type = schema.Type
		property.Format = schema.Format
	}
	switch {
	case property.Format == "date-time":
		return kindDateTime, nil
	case property.Format == "date":
		return kindDate, nil
	case property.Type == "string":
		return kindString, nil
	case property.Type == "number":
		return kindNumber, nil
	case property.Type == "integer":
		return kindInteger, nil
	case property.Type == "boolean":
		return kindBoolean, nil
	default:
		return 0, fmt.Errorf("unsupported scalar type=%q format=%q reference=%q", property.Type, property.Format, property.Reference)
	}
}

func snowflakeTypeForKind(kind scalarKind) string {
	switch kind {
	case kindString:
		return "VARCHAR"
	case kindNumber:
		return "FLOAT"
	case kindInteger:
		return "NUMBER(10,0)"
	case kindBoolean:
		return "BOOLEAN"
	case kindDate:
		return "DATE"
	case kindDateTime:
		return "TIMESTAMP_TZ"
	default:
		return ""
	}
}
