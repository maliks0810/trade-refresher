package services

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestDecodeWorkersRespectRuntimeParallelism(t *testing.T) {
	if got, maximum := effectiveDecodeWorkers(1000), runtime.GOMAXPROCS(0); got != maximum {
		t.Fatalf("effectiveDecodeWorkers(1000) = %d, want %d", got, maximum)
	}
	if got := effectiveDecodeWorkers(0); got != 1 {
		t.Fatalf("effectiveDecodeWorkers(0) = %d, want 1", got)
	}
}

func TestBuildTradeDatasetCreatesOneCompleteRowPerTrade(t *testing.T) {
	body := []byte(`{
		"nextPageToken": "next-page",
		"status": {"code": 0},
		"blockTrades": [{
			"tradeDate": "2026-07-08",
			"transactionType": "BUY",
			"multiFundId": 42,
			"version": 3,
			"asset": {
				"reference": {"assetId": "asset-1", "cusip": "123456789"},
				"summary": {"securityType": "BOND"}
			},
			"tradeComments": [{"comment": "complete", "commentType": "GENERAL"}],
			"trades": [{
				"id": "trade-1",
				"modifyTime": "2026-07-08T16:00:00Z",
				"portfolioReference": {"portfolioTicker": "P1"},
				"externalTradeReferences": [{"externalInvnum1": "ext-1"}],
				"origOrderReferences": [{
					"originalOrderId": "order-1",
					"externalOrderReferences": [{"externalId1": "client-order"}]
				}],
				"fxLegs": {
					"firstLeg": {"currencyCode": "USD", "settlementInstruction": 11},
					"secondLeg": {"currencyCode": "EUR", "settlementInstruction": 22}
				}
			}]
		}]
	}`)

	dataset, nextPageToken, err := BuildTradeDatasetFromFilterResponse(body, TradePageMetadata{
		PortfolioFilterType:  PortfolioFilterGroup,
		PortfolioFilterValue: "group:TCW_ALL",
		RefreshRunID:         "run-1",
		WatermarkStart:       time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC),
		WatermarkEnd:         time.Date(2026, 7, 8, 11, 3, 0, 0, time.UTC),
		PageNumber:           1,
		IngestedAt:           time.Date(2026, 7, 8, 11, 4, 0, 0, time.UTC),
		DecodeWorkers:        4,
	})
	if err != nil {
		t.Fatalf("BuildTradeDatasetFromFilterResponse returned error: %v", err)
	}
	if nextPageToken != "next-page" {
		t.Fatalf("nextPageToken = %q, want next-page", nextPageToken)
	}
	if dataset.BlockCount != 1 || dataset.TradeCount != 1 || dataset.TotalRows() != 1 {
		t.Fatalf("counts = blocks %d trades %d rows %d, want 1/1/1", dataset.BlockCount, dataset.TradeCount, dataset.TotalRows())
	}

	row := dataset.Rows[0].Values
	if row["TRADE_ID"] != "trade-1" || row["PORTFOLIO_NUMBER"] != "P1" || row["AS_OF_DATE"] != "2026-07-08" {
		t.Fatalf("trade fields not extracted: %#v", row)
	}
	if row["BLOCK_VERSION"] != int64(3) {
		t.Fatalf("documented block version was not extracted cleanly: %#v", row)
	}
	if _, exists := row["ALLOCATION_VERSION"]; exists {
		t.Fatal("undocumented allocation version should not be exposed as a consumer column")
	}
	if row["ASSET_EXPAND_REFERENCE_ASSET_ID"] != "asset-1" || row["ASSET_EXPAND_REFERENCE_CUSIP"] != "123456789" {
		t.Fatalf("asset fields not extracted: %#v", row)
	}
	if row["FX_FIRST_LEG_CURRENCY_CODE"] != "USD" || row["FX_SECOND_LEG_CURRENCY_CODE"] != "EUR" {
		t.Fatalf("FX fields not extracted: %#v", row)
	}
	if row["TRADE_COMMENTS_COMMENT"] == nil || row["EXTERNAL_TRADE_REFERENCES_EXTERNAL_INVNUM1"] == nil || row["ORIGINAL_ORDER_REFERENCES_ORIGINAL_ORDER_ID"] == nil {
		t.Fatalf("repeated fields were not projected into named columns: %#v", row)
	}
	if row["TRADE_CURRENT_KEY"] == "" || row["TRADE_VERSION_KEY"] == "" || row["BLOCK_CURRENT_KEY"] == "" {
		t.Fatalf("stable keys are missing: %#v", row)
	}

	var source map[string]any
	if err := json.Unmarshal([]byte(row["SOURCE_DATA"].(string)), &source); err != nil {
		t.Fatalf("SOURCE_DATA is not valid JSON: %v", err)
	}
	block := source["block"].(map[string]any)
	trade := source["trade"].(map[string]any)
	if _, duplicated := block["trades"]; duplicated {
		t.Fatal("SOURCE_DATA block should not duplicate the complete trades array")
	}
	if trade["id"] != "trade-1" || block["multiFundId"] != float64(42) {
		t.Fatalf("SOURCE_DATA did not preserve source fields: %#v", source)
	}
	if trade["modifyTime"] != "2026-07-08T09:00:00-07:00" {
		t.Fatalf("SOURCE_DATA modifyTime = %v, want Pacific time", trade["modifyTime"])
	}

	modifyTime := snowflakePacificTimestamp(row["MODIFY_TIME_PT"].(time.Time))
	_, offset := modifyTime.Zone()
	if modifyTime.Hour() != 9 || offset != -7*60*60 {
		t.Fatalf("modify time PT = %s, want 09:00 PDT", modifyTime)
	}
}

func TestBuildTradeDatasetPreservesUnknownSourceFields(t *testing.T) {
	body := []byte(`{"blockTrades":[{"futureBlockField":{"nested":true},"trades":[{"id":"1","futureTradeField":["a","b"],"futureTimestamp":"2026-01-08T16:00:00Z"}]}]}`)
	dataset, _, err := BuildTradeDatasetFromFilterResponse(body, TradePageMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	var source map[string]any
	if err := json.Unmarshal([]byte(dataset.Rows[0].Values["SOURCE_DATA"].(string)), &source); err != nil {
		t.Fatal(err)
	}
	block := source["block"].(map[string]any)
	trade := source["trade"].(map[string]any)
	if block["futureBlockField"] == nil || trade["futureTradeField"] == nil {
		t.Fatalf("future OpenAPI fields were not preserved: %#v", source)
	}
	if trade["futureTimestamp"] != "2026-01-08T08:00:00-08:00" {
		t.Fatalf("future timestamp = %v, want Pacific time", trade["futureTimestamp"])
	}
}

func TestBuildTradeDatasetAcceptsHTTP200EmbeddedStatusWithNoTrades(t *testing.T) {
	dataset, token, err := BuildTradeDatasetFromFilterResponse([]byte(`{
		"status":{"code":200,"message":"Number of results processed successfully: 0/0"},
		"blockTrades":[]
	}`), TradePageMetadata{})
	if err != nil {
		t.Fatalf("successful empty response returned error: %v", err)
	}
	if dataset.TotalRows() != 0 || token != "" {
		t.Fatalf("empty response returned rows=%d token=%q", dataset.TotalRows(), token)
	}
}

func TestBuildTradeDatasetRejectsEmbeddedLogicalError(t *testing.T) {
	_, _, err := BuildTradeDatasetFromFilterResponse([]byte(`{
		"status":{"code":3,"message":"invalid argument"},
		"blockTrades":[]
	}`), TradePageMetadata{})
	if err == nil || !strings.Contains(err.Error(), "embedded_status_code=3") {
		t.Fatalf("logical error response returned %v", err)
	}
}

func TestBuildTradeDatasetRejectsPartialEmbeddedSuccess(t *testing.T) {
	tests := []string{
		`{"status":{"code":200,"message":"Number of results processed successfully: 1/2"},"blockTrades":[]}`,
		`{"status":{"code":200,"message":"Number of results processed successfully: 1/1","details":[{"error":"portfolio failed"}]},"blockTrades":[]}`,
	}
	for _, body := range tests {
		if _, _, err := BuildTradeDatasetFromFilterResponse([]byte(body), TradePageMetadata{}); err == nil {
			t.Fatalf("partial response returned nil error: %s", body)
		}
	}
}

func TestRepeatedDateTimesAreProjectedInPacificTime(t *testing.T) {
	value := projectedCSV([]any{"2026-07-08T16:00:00Z"}, kindDateTime)
	if value != "2026-07-08T09:00:00-07:00" {
		t.Fatalf("projected timestamp = %v, want Pacific wall time", value)
	}
}

func TestRepeatedValuesUseRFC4180CSVWithoutArrayBrackets(t *testing.T) {
	value := projectedCSV([]any{"plain", "contains, comma", `contains "quote"`}, kindString)
	if want := `plain,"contains, comma","contains ""quote"""`; value != want {
		t.Fatalf("projected CSV = %q, want %q", value, want)
	}
}

func TestCommentsAndQuotesAreExtractedAsAlignedCSVColumns(t *testing.T) {
	body := []byte(`{
		"blockTrades": [{
			"tradeComments": [
				{"comment":"first","commentType":"GENERAL"},
				{"comment":"desk, follow-up","commentType":"OPERATIONS"}
			],
			"tradeQuotes": [
				{"bidPrice":99.125,"bidRate":1.25,"brokerId":"B1","brokerQuoteId":"Q1","offerPrice":99.5,"offerSpread":2.75,"quantity":1000,"status":"ACTIVE"},
				{"bidPrice":100.25,"brokerId":"B2","brokerQuoteId":"Q2","offerPrice":100.5,"offerSpread":3.5,"quantity":2000,"status":"EXPIRED"}
			],
			"trades": [{"id":"trade-1"}]
		}]
	}`)
	dataset, _, err := BuildTradeDatasetFromFilterResponse(body, TradePageMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	row := dataset.Rows[0].Values
	want := map[string]string{
		"TRADE_COMMENTS_COMMENT":       `first,"desk, follow-up"`,
		"TRADE_COMMENTS_COMMENT_TYPE":  "GENERAL,OPERATIONS",
		"TRADE_QUOTES_BID_PRICE":       "99.125,100.25",
		"TRADE_QUOTES_BID_RATE":        "1.25,",
		"TRADE_QUOTES_BROKER_ID":       "B1,B2",
		"TRADE_QUOTES_BROKER_QUOTE_ID": "Q1,Q2",
		"TRADE_QUOTES_OFFER_PRICE":     "99.5,100.5",
		"TRADE_QUOTES_OFFER_SPREAD":    "2.75,3.5",
		"TRADE_QUOTES_QUANTITY":        "1000,2000",
		"TRADE_QUOTES_STATUS":          "ACTIVE,EXPIRED",
	}
	for column, expected := range want {
		if got := fmt.Sprint(row[column]); got != expected {
			t.Fatalf("%s = %q, want %q", column, got, expected)
		}
		if strings.HasPrefix(fmt.Sprint(row[column]), "[") {
			t.Fatalf("%s still contains an array representation: %v", column, row[column])
		}
	}
}

func TestTradeDatasetDeduplicateKeepsNewestVersion(t *testing.T) {
	older := TradeTableRow{Values: map[string]any{
		"TRADE_CURRENT_KEY": "trade-1",
		"TRADE_VERSION_KEY": "v1",
		"MODIFY_TIME_PT":    time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC),
	}}
	newer := TradeTableRow{Values: map[string]any{
		"TRADE_CURRENT_KEY": "trade-1",
		"TRADE_VERSION_KEY": "v2",
		"MODIFY_TIME_PT":    time.Date(2026, 7, 8, 16, 1, 0, 0, time.UTC),
	}}
	dataset := TradeDataset{Rows: []TradeTableRow{newer, older, newer}}
	if removed := dataset.Deduplicate(); removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	if got := dataset.Rows[0].Values["TRADE_VERSION_KEY"]; got != "v2" {
		t.Fatalf("kept version = %v, want v2", got)
	}
}

func TestTradeDatasetDeduplicateUsesObservationTimeWhenModifyTimesTie(t *testing.T) {
	modified := time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC)
	olderObservation := TradeTableRow{Values: map[string]any{
		"TRADE_CURRENT_KEY": "trade-1", "TRADE_VERSION_KEY": "z-hash",
		"MODIFY_TIME_PT": modified, "INGESTED_AT_PT": modified.Add(time.Minute),
	}}
	newerObservation := TradeTableRow{Values: map[string]any{
		"TRADE_CURRENT_KEY": "trade-1", "TRADE_VERSION_KEY": "a-hash",
		"MODIFY_TIME_PT": modified, "INGESTED_AT_PT": modified.Add(2 * time.Minute),
	}}
	dataset := TradeDataset{Rows: []TradeTableRow{olderObservation, newerObservation}}
	dataset.Deduplicate()
	if got := dataset.Rows[0].Values["TRADE_VERSION_KEY"]; got != "a-hash" {
		t.Fatalf("kept version = %v, want latest observation a-hash", got)
	}
}

func TestSparseTradesReceiveDistinctKeys(t *testing.T) {
	body := []byte(`{"blockTrades":[{"trades":[{},{}]}]}`)
	dataset, _, err := BuildTradeDatasetFromFilterResponse(body, TradePageMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	if dataset.TotalRows() != 2 {
		t.Fatalf("rows = %d, want 2", dataset.TotalRows())
	}
	if dataset.Rows[0].Values["TRADE_CURRENT_KEY"] == dataset.Rows[1].Values["TRADE_CURRENT_KEY"] {
		t.Fatal("sparse trades received the same key")
	}
}
