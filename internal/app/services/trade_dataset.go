package services

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type TradeDataset struct {
	Rows              []TradeTableRow
	BlockCount        int
	TradeCount        int
	DuplicatesRemoved int
}

func NewTradeDataset() TradeDataset {
	return TradeDataset{}
}

func (d TradeDataset) TotalRows() int {
	return len(d.Rows)
}

func (d TradeDataset) Append(other TradeDataset) TradeDataset {
	d.Rows = append(d.Rows, other.Rows...)
	d.BlockCount += other.BlockCount
	d.TradeCount += other.TradeCount
	d.DuplicatesRemoved += other.DuplicatesRemoved
	return d
}

func (d *TradeDataset) Deduplicate() int {
	if d == nil || len(d.Rows) < 2 {
		return 0
	}

	originalCount := len(d.Rows)
	indexes := make(map[string]int, originalCount)
	rows := d.Rows[:0]
	for _, row := range d.Rows {
		key, _ := row.Values["TRADE_CURRENT_KEY"].(string)
		if key == "" {
			rows = append(rows, row)
			continue
		}
		if index, exists := indexes[key]; exists {
			if shouldReplaceTradeRow(rows[index], row) {
				rows[index] = row
			}
			continue
		}
		indexes[key] = len(rows)
		rows = append(rows, row)
	}
	d.Rows = rows
	removed := originalCount - len(rows)
	d.DuplicatesRemoved += removed
	return removed
}

func shouldReplaceTradeRow(existing, candidate TradeTableRow) bool {
	return tradeRank(candidate).replaces(tradeRank(existing))
}

type tradeRowRank struct {
	modified    time.Time
	hasModified bool
	ingested    time.Time
}

func tradeRank(row TradeTableRow) tradeRowRank {
	modified, hasModified := row.Values["MODIFY_TIME_PT"].(time.Time)
	return tradeRowRank{
		modified:    modified,
		hasModified: hasModified,
		ingested:    timeValue(row.Values["INGESTED_AT_PT"]),
	}
}

func (candidate tradeRowRank) replaces(existing tradeRowRank) bool {
	if candidate.hasModified && (!existing.hasModified || candidate.modified.After(existing.modified)) {
		return true
	}
	if existing.hasModified && candidate.hasModified && candidate.modified.Before(existing.modified) {
		return false
	}
	return candidate.ingested.After(existing.ingested)
}

func timeValue(value any) time.Time {
	timestamp, _ := value.(time.Time)
	return timestamp
}

type TradeTableRow struct {
	Values map[string]any
}

type TradePageMetadata struct {
	PortfolioFilterType  PortfolioFilterType
	PortfolioFilterValue string
	RefreshRunID         string
	WatermarkStart       time.Time
	WatermarkEnd         time.Time
	PageNumber           int
	IngestedAt           time.Time
	DecodeWorkers        int
}

type TradeTableDefinition struct {
	Columns      []ColumnDefinition
	columnByName map[string]ColumnDefinition
}

type ColumnDefinition struct {
	Name       string
	SQLType    string
	IsPayload  bool
	IsDateTime bool
	Comment    string
}

type filterTradesResponse struct {
	BlockTrades   []json.RawMessage `json:"blockTrades"`
	NextPageToken string            `json:"nextPageToken"`
	Status        *rpcStatus        `json:"status"`
}

type rpcStatus struct {
	Code    int               `json:"code"`
	Message string            `json:"message"`
	Details []json.RawMessage `json:"details"`
}

type tradeFilterStatusError struct {
	Code         int
	Message      string
	DetailsCount int
	Processed    int
	Total        int
	HasCounts    bool
}

func (e *tradeFilterStatusError) Error() string {
	if e.HasCounts {
		return fmt.Sprintf("BlackRock trade filter processed %d/%d results (embedded_status_code=%d message=%q details=%d)", e.Processed, e.Total, e.Code, e.Message, e.DetailsCount)
	}
	return fmt.Sprintf("BlackRock trade filter returned embedded_status_code=%d message=%q details=%d", e.Code, e.Message, e.DetailsCount)
}

type extractedBlock struct {
	dataset TradeDataset
	err     error
}

func BuildTradeDatasetFromFilterResponse(body []byte, meta TradePageMetadata) (TradeDataset, string, error) {
	var response filterTradesResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&response); err != nil {
		return TradeDataset{}, "", err
	}
	if response.Status != nil && response.Status.Code != 0 && response.Status.Code != http.StatusOK {
		return TradeDataset{}, response.NextPageToken, &tradeFilterStatusError{Code: response.Status.Code, Message: response.Status.Message, DetailsCount: len(response.Status.Details)}
	}
	if response.Status != nil {
		if len(response.Status.Details) > 0 {
			return TradeDataset{}, response.NextPageToken, &tradeFilterStatusError{Code: response.Status.Code, Message: response.Status.Message, DetailsCount: len(response.Status.Details)}
		}
		var processed, total int
		if count, _ := fmt.Sscanf(response.Status.Message, "Number of results processed successfully: %d/%d", &processed, &total); count == 2 && processed != total {
			return TradeDataset{}, response.NextPageToken, &tradeFilterStatusError{Code: response.Status.Code, Message: response.Status.Message, Processed: processed, Total: total, HasCounts: true}
		}
	}
	if meta.IngestedAt.IsZero() {
		meta.IngestedAt = time.Now()
	}
	if len(response.BlockTrades) == 0 {
		return NewTradeDataset(), response.NextPageToken, nil
	}

	results := make([]extractedBlock, len(response.BlockTrades))
	workers := effectiveDecodeWorkers(meta.DecodeWorkers)
	group := new(errgroup.Group)
	group.SetLimit(workers)
	for i, payload := range response.BlockTrades {
		i := i
		payload := payload
		group.Go(func() error {
			dataset, err := buildTradeDataset(payload, i, meta)
			results[i] = extractedBlock{dataset: dataset, err: err}
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return TradeDataset{}, response.NextPageToken, err
	}

	dataset := NewTradeDataset()
	for _, result := range results {
		if result.err != nil {
			return TradeDataset{}, response.NextPageToken, result.err
		}
		dataset = dataset.Append(result.dataset)
	}
	return dataset, response.NextPageToken, nil
}

func effectiveDecodeWorkers(configured int) int {
	if configured < 1 {
		configured = 1
	}
	if available := runtime.GOMAXPROCS(0); configured > available {
		return available
	}
	return configured
}

func buildTradeDataset(payload json.RawMessage, ordinal int, meta TradePageMetadata) (TradeDataset, error) {
	var block map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&block); err != nil {
		return TradeDataset{}, err
	}
	normalizeSourceDateTimes(block)

	parent := cloneMap(block)
	delete(parent, "trades")
	parentPayload := mustJSON(parent)
	blockCurrentKey := blockCurrentKey(block)
	blockVersionKey := hashParts(blockCurrentKey, parentPayload)
	blockValues := make(map[string]any)
	setBlockColumns(blockValues, block)
	setCollectionColumns(blockValues, block, nil, "block")
	dataset := TradeDataset{BlockCount: 1}
	for tradeOrdinal, trade := range arrayValues(block["trades"]) {
		tradeCurrentKey := tradeKeyForBlock(trade, blockCurrentKey, tradeOrdinal)
		sourceData := mustJSON(map[string]any{"block": parent, "trade": trade})
		tradeVersionKey := hashParts(tradeCurrentKey, sourceData)
		values := baseValues(meta, blockCurrentKey, blockVersionKey, tradeCurrentKey, tradeVersionKey, ordinal, tradeOrdinal, sourceData)
		for key, value := range blockValues {
			values[key] = value
		}
		setTradeColumns(values, trade)
		setCollectionColumns(values, nil, trade, "trade")
		dataset.Rows = append(dataset.Rows, TradeTableRow{Values: values})
		dataset.TradeCount++
	}
	return dataset, nil
}

func baseValues(meta TradePageMetadata, blockCurrentKey, blockVersionKey, tradeCurrentKey, tradeVersionKey string, blockOrdinal, tradeOrdinal int, sourceData string) map[string]any {
	return map[string]any{
		"BLOCK_CURRENT_KEY":      blockCurrentKey,
		"BLOCK_VERSION_KEY":      blockVersionKey,
		"TRADE_CURRENT_KEY":      tradeCurrentKey,
		"TRADE_VERSION_KEY":      tradeVersionKey,
		"PORTFOLIO_FILTER_TYPE":  string(meta.PortfolioFilterType),
		"PORTFOLIO_FILTER_VALUE": meta.PortfolioFilterValue,
		"REFRESH_RUN_ID":         meta.RefreshRunID,
		"WATERMARK_START_PT":     meta.WatermarkStart,
		"WATERMARK_END_PT":       meta.WatermarkEnd,
		"PAGE_NUMBER":            meta.PageNumber,
		"BLOCK_ORDINAL":          blockOrdinal,
		"TRADE_ORDINAL":          tradeOrdinal,
		"INGESTED_AT_PT":         meta.IngestedAt,
		"SOURCE_DATA":            sourceData,
	}
}

func setCollectionColumns(values, block, trade map[string]any, rootName string) {
	root := map[string]any{"block": block, "trade": trade}
	for _, spec := range repeatedFieldColumns() {
		if len(spec.Path) == 0 || spec.Path[0] != rootName {
			continue
		}
		if value, ok := projectJSONPath(root, spec.Path); ok {
			values[spec.Column] = projectedCSV(value, spec.Kind)
		}
	}
}

type repeatedFieldColumn struct {
	Column string
	Path   []string
	Kind   scalarKind
}

var (
	repeatedFieldColumnsOnce sync.Once
	repeatedFieldColumnsList []repeatedFieldColumn
)

func repeatedFieldColumns() []repeatedFieldColumn {
	repeatedFieldColumnsOnce.Do(func() {
		repeatedFieldColumnsList = buildRepeatedFieldColumns()
	})
	return repeatedFieldColumnsList
}

func buildRepeatedFieldColumns() []repeatedFieldColumn {
	var columns []repeatedFieldColumn
	columns = appendRepeatedFields(columns, "TRADE_COMMENTS", []string{"block", "tradeComments"}, commentFieldSpecs)
	columns = appendRepeatedFields(columns, "TRADE_QUOTES", []string{"block", "tradeQuotes"}, quoteFieldSpecs)
	columns = appendRepeatedFields(columns, "USER_DEFINED_FIELDS", []string{"block", "userDefinedFields"}, userDefinedFieldSpecs)
	columns = appendRepeatedFields(columns, "COLLATERALS", []string{"trade", "collaterals"}, collateralFieldSpecs)
	columns = appendRepeatedFields(columns, "CONTRACTS", []string{"trade", "contracts"}, contractFieldSpecs)
	columns = appendRepeatedFields(columns, "EXTERNAL_TRADE_REFERENCES", []string{"trade", "externalTradeReferences"}, externalReferenceFieldSpecs)
	columns = appendRepeatedFields(columns, "FORWARD_EXTERNAL_TRADE_REFERENCES", []string{"trade", "forwardExternalTradeReferences"}, externalReferenceFieldSpecs)
	columns = appendRepeatedFields(columns, "TRADE_CHARGES", []string{"trade", "tradeCharges"}, chargeFieldSpecs)
	columns = appendRepeatedFields(columns, "TRADE_RELATIONSHIPS", []string{"trade", "tradeRelationships"}, relationshipFieldSpecs)

	columns = appendReferenceFields(columns, "COLLATERALS_ASSET_REFERENCE", []string{"trade", "collaterals", "assetReference"}, []string{"assetId", "bloombergTicker", "cins", "cusip", "isin", "sedol"})
	columns = appendReferenceFields(columns, "CONTRACTS_ASSET_REFERENCE", []string{"trade", "contracts", "assetReference"}, []string{"assetId", "bloombergTicker", "cins", "cusip", "isin", "sedol"})
	columns = appendReferenceFields(columns, "TRADE_RELATIONSHIPS_PORTFOLIO_1", []string{"trade", "tradeRelationships", "portfolioReference1"}, []string{"portfolioId", "portfolioTicker"})
	columns = appendReferenceFields(columns, "TRADE_RELATIONSHIPS_PORTFOLIO_2", []string{"trade", "tradeRelationships", "portfolioReference2"}, []string{"portfolioId", "portfolioTicker"})
	columns = append(columns,
		repeatedFieldColumn{Column: "ORIGINAL_ORDER_REFERENCES_ORIGINAL_ORDER_ID", Path: []string{"trade", "origOrderReferences", "originalOrderId"}, Kind: kindString},
		repeatedFieldColumn{Column: "ORIGINAL_ORDER_REFERENCES_EXTERNAL_ACCOUNT_CODE", Path: []string{"trade", "origOrderReferences", "externalOrderReferences", "accountCode"}, Kind: kindString},
		repeatedFieldColumn{Column: "ORIGINAL_ORDER_REFERENCES_EXTERNAL_ID1", Path: []string{"trade", "origOrderReferences", "externalOrderReferences", "externalId1"}, Kind: kindString},
		repeatedFieldColumn{Column: "ORIGINAL_ORDER_REFERENCES_EXTERNAL_ID2", Path: []string{"trade", "origOrderReferences", "externalOrderReferences", "externalId2"}, Kind: kindString},
	)
	return columns
}

func appendRepeatedFields(columns []repeatedFieldColumn, prefix string, root []string, specs []fieldSpec) []repeatedFieldColumn {
	for _, spec := range specs {
		path := append(append([]string{}, root...), spec.Path...)
		columns = append(columns, repeatedFieldColumn{Column: prefix + "_" + spec.Column, Path: path, Kind: spec.Kind})
	}
	return columns
}

func appendReferenceFields(columns []repeatedFieldColumn, prefix string, root, properties []string) []repeatedFieldColumn {
	for _, property := range properties {
		path := append(append([]string{}, root...), property)
		columns = append(columns, repeatedFieldColumn{Column: prefix + "_" + toSnake(property), Path: path, Kind: kindString})
	}
	return columns
}

func projectedCSV(value any, kind scalarKind) string {
	values := make([]string, 0, 4)
	appendProjectedCSVValues(&values, value, kind)
	var builder strings.Builder
	for index, value := range values {
		if index > 0 {
			builder.WriteByte(',')
		}
		if strings.ContainsAny(value, ",\"\r\n") {
			builder.WriteByte('"')
			builder.WriteString(strings.ReplaceAll(value, "\"", "\"\""))
			builder.WriteByte('"')
			continue
		}
		builder.WriteString(value)
	}
	return builder.String()
}

func appendProjectedCSVValues(values *[]string, value any, kind scalarKind) {
	if array, ok := value.([]any); ok {
		for _, item := range array {
			appendProjectedCSVValues(values, item, kind)
		}
		return
	}
	if value == nil {
		*values = append(*values, "")
		return
	}
	if kind == kindDateTime {
		if parsed, ok := parseFlexibleTime(fmt.Sprint(value)); ok {
			*values = append(*values, pacificLogTimestamp(parsed))
			return
		}
		*values = append(*values, "")
		return
	}
	normalized := normalizeValue(value, kind)
	if normalized == nil {
		*values = append(*values, "")
		return
	}
	*values = append(*values, fmt.Sprint(normalized))
}

func projectJSONPath(value any, path []string) (any, bool) {
	if len(path) == 0 {
		return value, true
	}
	switch typed := value.(type) {
	case map[string]any:
		next, ok := typed[path[0]]
		if !ok {
			return nil, false
		}
		return projectJSONPath(next, path[1:])
	case []any:
		projected := make([]any, len(typed))
		found := false
		for index, item := range typed {
			if result, ok := projectJSONPath(item, path); ok {
				projected[index] = result
				found = true
			}
		}
		return projected, found
	default:
		return nil, false
	}
}

func cloneMap(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func normalizeSourceDateTimes(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if text, ok := child.(string); ok && isTimestampCandidate(text) {
				if parsed, isTimestamp := parseFlexibleTime(text); isTimestamp {
					typed[key] = pacificLogTimestamp(parsed)
					continue
				}
			}
			normalizeSourceDateTimes(child)
		}
	case []any:
		for _, child := range typed {
			normalizeSourceDateTimes(child)
		}
	}
}

func isTimestampCandidate(value string) bool {
	return len(value) >= 19 && value[4] == '-' && value[7] == '-' && (value[10] == 'T' || value[10] == ' ')
}

func setBlockColumns(values map[string]any, block map[string]any) {
	setScalars(values, block, blockFieldSpecs)
	setAssetReference(values, "ASSET_REFERENCE", objectMap(block["assetReference"]))
	asset := objectMap(block["asset"])
	setAssetReference(values, "ASSET_EXPAND_REFERENCE", objectMap(asset["reference"]))
	setAssetReference(values, "ASSET_SUMMARY_REFERENCE", objectMap(objectMap(asset["summary"])["reference"]))
	setScalars(values, objectMap(asset["summary"]), assetSummaryFieldSpecs)
	setBrokerReference(values, "BROKER", objectMap(block["brokerReference"]))
	setBrokerReference(values, "EXECUTING_BROKER", objectMap(block["executingBrokerReference"]))
	setAssetReference(values, "FORWARD_ASSET_REFERENCE", objectMap(block["forwardAssetReference"]))
	setPortfolioReference(values, "TRANSFER_PORTFOLIO", objectMap(block["transferPortfolioReference"]))
	values["SOURCE_MODIFY_TIME_PT"] = maxTradeModifyTime(block)
	values["TRADE_COUNT"] = len(arrayValues(block["trades"]))
}

func setTradeColumns(values map[string]any, trade map[string]any) {
	setScalars(values, trade, tradeFieldSpecs)
	setPrimaryPortfolioReference(values, objectMap(trade["portfolioReference"]))
	setStrategyReference(values, objectMap(trade["strategyReference"]))
	setTradeFlags(values, objectMap(trade["tradeFlags"]))
	fxLegs := objectMap(trade["fxLegs"])
	setFXLeg(values, "FX_FIRST_LEG", objectMap(fxLegs["firstLeg"]))
	setFXLeg(values, "FX_SECOND_LEG", objectMap(fxLegs["secondLeg"]))
}

func setPrimaryPortfolioReference(values map[string]any, reference map[string]any) {
	if value, ok := reference["portfolioId"]; ok {
		values["PORTFOLIO_ID"] = normalizeValue(value, kindString)
	}
	if value, ok := reference["portfolioTicker"]; ok {
		values["PORTFOLIO_NUMBER"] = normalizeValue(value, kindString)
	}
}

type scalarKind int

const (
	kindString scalarKind = iota
	kindNumber
	kindInteger
	kindBoolean
	kindDate
	kindDateTime
)

type fieldSpec struct {
	Column string
	Path   []string
	Kind   scalarKind
}

func field(column string, kind scalarKind, path ...string) fieldSpec {
	if len(path) == 0 {
		path = []string{lowerCamel(column)}
	}
	return fieldSpec{Column: column, Path: path, Kind: kind}
}

func setScalars(values map[string]any, object map[string]any, specs []fieldSpec) {
	for _, spec := range specs {
		value, ok := valueAt(object, spec.Path...)
		if !ok {
			continue
		}
		values[spec.Column] = normalizeValue(value, spec.Kind)
	}
}

func normalizeValue(value any, kind scalarKind) any {
	if value == nil {
		return nil
	}
	switch kind {
	case kindString:
		if text := fmt.Sprint(value); text != "" {
			return text
		}
	case kindNumber:
		return fmt.Sprint(value)
	case kindInteger:
		switch typed := value.(type) {
		case json.Number:
			if integer, err := typed.Int64(); err == nil {
				return integer
			}
		case float64:
			return int64(typed)
		case int:
			return int64(typed)
		case int64:
			return typed
		default:
			if integer, err := strconv.ParseInt(fmt.Sprint(value), 10, 64); err == nil {
				return integer
			}
		}
	case kindBoolean:
		if boolean, ok := value.(bool); ok {
			return boolean
		}
	case kindDate:
		if text := fmt.Sprint(value); text != "" {
			return text
		}
	case kindDateTime:
		if parsed, ok := parseFlexibleTime(fmt.Sprint(value)); ok {
			return parsed
		}
	}
	return nil
}

func setAssetReference(values map[string]any, prefix string, reference map[string]any) {
	setPrefixed(values, prefix, reference, []string{"assetId", "bloombergTicker", "cins", "cusip", "isin", "sedol"})
}

func setBrokerReference(values map[string]any, prefix string, reference map[string]any) {
	if value, ok := reference["brokerId"]; ok {
		values[prefix+"_ID"] = normalizeValue(value, kindString)
	}
	if value, ok := reference["brokerShortname"]; ok {
		values[prefix+"_SHORTNAME"] = normalizeValue(value, kindString)
	}
	if value, ok := reference["brokerTicker"]; ok {
		values[prefix+"_TICKER"] = normalizeValue(value, kindString)
	}
}

func setPortfolioReference(values map[string]any, prefix string, reference map[string]any) {
	if value, ok := reference["portfolioId"]; ok {
		values[prefix+"_ID"] = normalizeValue(value, kindString)
	}
	if value, ok := reference["portfolioTicker"]; ok {
		values[prefix+"_TICKER"] = normalizeValue(value, kindString)
	}
}

func setStrategyReference(values map[string]any, reference map[string]any) {
	if value, ok := reference["strategyId"]; ok {
		values["STRATEGY_ID"] = normalizeValue(value, kindString)
	}
	if value, ok := reference["strategyName"]; ok {
		values["STRATEGY_NAME"] = normalizeValue(value, kindString)
	}
}

func setFXLeg(values map[string]any, prefix string, leg map[string]any) {
	if value, ok := leg["currencyCode"]; ok {
		values[prefix+"_CURRENCY_CODE"] = normalizeValue(value, kindString)
	}
	if value, ok := leg["settlementInstruction"]; ok {
		values[prefix+"_SETTLEMENT_INSTRUCTION"] = normalizeValue(value, kindInteger)
	}
}

func setTradeFlags(values map[string]any, flags map[string]any) {
	for _, key := range []string{"allowDirtyPrice", "beneficialOwnershipChange", "cashHaircut", "compliancePending", "deliveryFreePayment", "electronicPoolNotificationEligible", "fullyAllocated", "netted", "outsideCommitment", "suppressContract"} {
		if value, ok := flags[key].(bool); ok {
			values["FLAG_"+toSnake(key)] = value
		}
	}
}

func setPrefixed(values map[string]any, prefix string, object map[string]any, keys []string) {
	for _, key := range keys {
		if value, ok := object[key]; ok {
			values[prefix+"_"+toSnake(key)] = normalizeValue(value, kindString)
		}
	}
}

func valueAt(object map[string]any, path ...string) (any, bool) {
	current := any(object)
	for _, part := range path {
		next, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = next[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func objectMap(value any) map[string]any {
	if object, ok := value.(map[string]any); ok {
		return object
	}
	return map[string]any{}
}

func arrayValues(value any) []map[string]any {
	array, ok := value.([]any)
	if !ok {
		return nil
	}
	values := make([]map[string]any, 0, len(array))
	for _, item := range array {
		if object, ok := item.(map[string]any); ok {
			values = append(values, object)
		}
	}
	return values
}

func scalarString(object map[string]any, key string) string {
	if value, ok := object[key]; ok {
		return fmt.Sprint(value)
	}
	return ""
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func blockCurrentKey(block map[string]any) string {
	if multiFundID := scalarString(block, "multiFundId"); multiFundID != "" {
		return hashParts("BLOCK_MULTI_FUND_ID", multiFundID)
	}
	if id := scalarString(block, "id"); id != "" {
		return hashParts("BLOCK_ID", id)
	}
	trades := arrayValues(block["trades"])
	tradeKeys := make([]string, 0, len(trades))
	for _, trade := range trades {
		tradeKeys = append(tradeKeys, tradeKey(trade))
	}
	sort.Strings(tradeKeys)
	return hashParts(
		"BLOCK",
		scalarString(block, "tradeDate"),
		scalarString(block, "transactionType"),
		scalarString(block, "multiFundId"),
		strings.Join(tradeKeys, "|"),
	)
}

func tradeKey(trade map[string]any) string {
	if id := scalarString(trade, "id"); id != "" {
		return hashParts("TRADE_ID", id)
	}
	portfolio := ""
	if ref, ok := trade["portfolioReference"].(map[string]any); ok {
		portfolio = scalarString(ref, "portfolioId") + "|" + scalarString(ref, "portfolioTicker")
	}
	return hashParts("TRADE", portfolio, scalarString(trade, "invnum"), scalarString(trade, "tradeNumber"))
}

func tradeKeyForBlock(trade map[string]any, blockKey string, ordinal int) string {
	if id := scalarString(trade, "id"); id != "" {
		return hashParts("TRADE_ID", id)
	}
	portfolio := ""
	if ref, ok := trade["portfolioReference"].(map[string]any); ok {
		portfolio = scalarString(ref, "portfolioId") + "|" + scalarString(ref, "portfolioTicker")
	}
	invnum := scalarString(trade, "invnum")
	tradeNumber := scalarString(trade, "tradeNumber")
	if portfolio != "" || invnum != "" || tradeNumber != "" {
		return hashParts("TRADE", portfolio, invnum, tradeNumber)
	}
	return hashParts("TRADE_BLOCK_ORDINAL", blockKey, fmt.Sprint(ordinal))
}

func maxTradeModifyTime(block map[string]any) time.Time {
	var max time.Time
	for _, trade := range arrayValues(block["trades"]) {
		value, ok := trade["modifyTime"].(string)
		if !ok {
			continue
		}
		parsed, ok := parseFlexibleTime(value)
		if !ok {
			continue
		}
		if max.IsZero() || parsed.After(max) {
			max = parsed
		}
	}
	return max
}

func hashParts(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}

func parseFlexibleTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, true
		}
	}
	for _, layout := range []string{"2006-01-02 15:04:05.999", "2006-01-02 15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, value, pacificLocation()); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func lowerCamel(column string) string {
	parts := strings.Split(strings.ToLower(column), "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] == "" {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

func toSnake(value string) string {
	var builder strings.Builder
	for i, r := range value {
		if i > 0 && r >= 'A' && r <= 'Z' {
			builder.WriteByte('_')
		}
		builder.WriteRune(r)
	}
	return strings.ToUpper(builder.String())
}

var blockFieldSpecs = []fieldSpec{
	field("ACCOUNTING_DESIGNATION", kindString), field("ACTUAL_SETTLEMENT_DATE", kindDate), field("AUTHORIZED_TIME_PT", kindDateTime, "authorizedTime"),
	field("BROKER_DESK_ID", kindString), field("BROKER_REASON", kindString), field("COLLATERAL_EXPOSURE_TYPE", kindString),
	field("CONFIRMED_BY", kindString), field("CONFIRMED_WITH", kindString), field("CONVEXITY", kindNumber), field("COUPON", kindNumber),
	field("COUPON_TYPE", kindString), field("DAYS_TO_MATURITY_2A7", kindInteger, "daysToMaturity2a7"), field("DEALING_CAPACITY", kindString),
	field("DROP_RATE", kindNumber), field("DURATION", kindNumber), field("EFFECTIVE_RATE", kindNumber), field("ENTERED_BY", kindString),
	field("ENTRY_TIME_PT", kindDateTime, "entryTime"), field("EXECUTING_BROKER_DESK_ID", kindString), field("EXECUTION_TIME_PT", kindDateTime, "executionTime"),
	field("EXECUTION_TIME_SOURCE", kindString), field("FACTOR", kindNumber), field("FACTOR_DATE", kindDate), field("FORWARD_POINTS", kindNumber),
	field("FORWARD_PRICE", kindNumber), field("FX_SPOT_PRICE", kindNumber, "fxSpotPrice"), field("MULTI_FUND_ID", kindInteger),
	field("PERCENT_YIELD", kindNumber), field("PLACEMENT_ID", kindString), field("POOL_STIPULATIONS", kindString), field("PREPAY_TYPE", kindString),
	field("PRICE", kindNumber), field("PRICE_TYPE", kindString), field("PRICING_INDEX", kindString), field("PRICING_SPREAD", kindNumber),
	field("PSA", kindNumber), field("PURPOSE", kindString), field("REINVESTMENT_RATE", kindNumber), field("REVIEW_TIME_PT", kindDateTime, "reviewTime"),
	field("SETTLEMENT_CURRENCY", kindString), field("SETTLEMENT_CURRENCY_PAIR", kindString), field("SETTLEMENT_DATE", kindDate),
	field("SETTLEMENT_FX_RATE", kindNumber), field("STATUS", kindString), field("SUPPRESS_EXTERNAL_NOTIFICATION", kindBoolean),
	field("SWAP_POINTS", kindNumber), field("TBA_STIPULATIONS", kindString), field("BLOCK_TOUCH_COUNT", kindInteger, "touchCount"), field("AS_OF_DATE", kindDate, "tradeDate"),
	field("TRADE_DISCOUNT_PRICE", kindNumber), field("TRADE_YIELD_TO_CALL", kindNumber), field("TRADER_INITIALS", kindString),
	field("TRANSACTION_TYPE", kindString), field("TRANSFER_EXTERNAL_ACCOUNT_NAME", kindString), field("BLOCK_VERSION", kindInteger, "version"),
	field("WEIGHTED_AVERAGE_COUPON", kindNumber), field("WEIGHTED_AVERAGE_MATURITY", kindInteger),
}

var assetSummaryFieldSpecs = []fieldSpec{
	field("ASSET_SUMMARY_ISSUE_COUNTRY_CODE", kindString, "issueCountryCode"),
	field("ASSET_SUMMARY_SECURITY_CURRENCY", kindString, "securityCurrency"),
	field("ASSET_SUMMARY_SECURITY_GROUP", kindString, "securityGroup"),
	field("ASSET_SUMMARY_SECURITY_TYPE", kindString, "securityType"),
}

var tradeFieldSpecs = []fieldSpec{
	field("ADDITIONAL_TRADE_CHARGE", kindNumber), field("ALLOCATION_AMOUNT", kindNumber), field("COMMISSION", kindNumber),
	field("CURRENT_FACTORED_DOWN_AMOUNT", kindNumber), field("EFFECTIVE_TERM_DATE", kindDate), field("EXCHANGE", kindString),
	field("EXCHANGE_RATE", kindNumber), field("FEE", kindNumber), field("FORWARD_INTEREST", kindNumber), field("FORWARD_PRINCIPAL", kindNumber),
	field("FORWARD_QUANTITY", kindNumber), field("FORWARD_SETTLEMENT_INSTRUCTION_ID1", kindString), field("TRADE_ID", kindString, "id"),
	field("INTEREST", kindNumber), field("INTEREST_AT_MATURITY", kindNumber), field("INVNUM", kindInteger), field("MODIFY_TIME_PT", kindDateTime, "modifyTime"),
	field("MORTGAGE_BACKED_SECURITIES_CLEARING_CORPORATION_TYPE", kindString, "mortgageBackedSecuritiesClearingCorporationType"),
	field("ORDER_ID", kindString), field("PRINCIPAL", kindNumber), field("QUANTITY", kindNumber), field("ROLL_FEE", kindNumber),
	field("SERIES_NUMBER", kindInteger), field("SETTLEMENT_INSTRUCTION_ID1", kindString), field("TRADE_NUMBER", kindInteger),
}

var commentFieldSpecs = []fieldSpec{field("COMMENT", kindString), field("COMMENT_TYPE", kindString)}
var userDefinedFieldSpecs = []fieldSpec{field("FIELD_LABEL", kindString), field("FIELD_VALUE", kindString)}
var quoteFieldSpecs = []fieldSpec{
	field("BID_PRICE", kindNumber), field("BID_RATE", kindNumber), field("BID_SPREAD", kindNumber), field("BROKER_ID", kindString),
	field("BROKER_QUOTE_ID", kindString), field("OFFER_PRICE", kindNumber), field("OFFER_RATE", kindNumber), field("OFFER_SPREAD", kindNumber),
	field("QUANTITY", kindNumber), field("STATUS", kindString),
}
var collateralFieldSpecs = []fieldSpec{
	field("CASH_PERCENTAGE", kindNumber), field("COLLATERAL_PERCENTAGE", kindNumber), field("EFFECTIVE_TERMINATION_DATE", kindDate),
	field("INTEREST", kindNumber), field("PRICE", kindNumber), field("PRINCIPAL", kindNumber), field("QUANTITY", kindNumber), field("TERMINATION_DATE", kindDate),
}
var contractFieldSpecs = []fieldSpec{field("CURRENCY_CODE", kindString), field("INVNUM", kindInteger), field("QUANTITY", kindNumber), field("TRADE_NUMBER", kindInteger)}
var externalReferenceFieldSpecs = []fieldSpec{field("ACCOUNT_CODE", kindString), field("EXTERNAL_INVNUM1", kindString), field("EXTERNAL_INVNUM2", kindString)}
var originalOrderExternalFieldSpecs = []fieldSpec{field("ACCOUNT_CODE", kindString), field("EXTERNAL_ID1", kindString), field("EXTERNAL_ID2", kindString)}
var chargeFieldSpecs = []fieldSpec{field("AMOUNT", kindNumber), field("CALCULATION_TYPE", kindString), field("CATEGORY", kindString), field("RATE", kindNumber), field("RATE_TYPE", kindString)}
var relationshipFieldSpecs = []fieldSpec{
	field("AMOUNT", kindNumber), field("APPLY_TIME_PT", kindDateTime, "applyTime"), field("INVNUM1", kindInteger), field("INVNUM2", kindInteger),
	field("TRADE_RELATIONSHIP_TYPE", kindString), field("VERSION1", kindInteger), field("VERSION2", kindInteger),
}
