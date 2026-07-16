# trade-refresher

Refreshes BlackRock Aladdin trades into Snowflake and proxies every Trade API operation for AKS-internal callers.

## API

```sh
# Local
BASE_URL="http://localhost:8100/de/v1/api"

# Same AKS cluster
BASE_URL="http://trade-refresher.<namespace>.svc.cluster.local/de/v1/api"

API_KEY="<trade-refresher-api-key value>"
CORRELATION_ID="investment-operations" # UUID, team name, or email
```

Every proxy request requires `Authorization: Bearer <API_KEY>` and a caller-supplied `X-Correlation-ID` of 1-256 bytes. Every `POST` also requires `Content-Type: application/json` and a valid JSON body. The correlation ID is returned and logged; credentials and trade payloads are not logged.

| Endpoint | Required input | Returns |
| --- | --- | --- |
| `GET /longrunningoperations/{id}` | Batch operation `id` | Operation status and completed batch result |
| `POST /trades:filter` | Filter `query`; optional `options`, `expands`, `pageSize`, `pageToken` | Block trades, status, and optional `nextPageToken` |
| `POST /trades:retrieve` | One of `tradeKeys`, `externTradeKeys`, or `orderKeys` | Matching block trades and status |
| `POST /trades:post` | One `blockTrade`; optional post `config` | Post/update result |
| `POST /trades:batchPost` | `blockTrades` array; optional post `config` | Long-running operation |
| `POST /trades:cancel` | One `tradeKey`; optional cancel `config` | Cancellation result |
| `POST /trades:batchCancel` | `tradeKeys` array; optional cancel `config` | Long-running operation |

Use local Swagger for complete request schemas and field descriptions:

- `http://localhost:8100/de/v1/api/docs/`
- `http://localhost:8100/de/v1/api/openapi.json`

Swagger is disabled in AKS when `INTERNAL_INGRESS_ENABLED=false`. When internal ingress is enabled, use `https://<internal-ingress-host>/de/v1/api/docs/`; Swagger is not intended for `svc.cluster.local` access.

### Example

```sh
curl --fail-with-body --request POST "$BASE_URL/trades:filter" \
  --header "Authorization: Bearer $API_KEY" \
  --header "X-Correlation-ID: $CORRELATION_ID" \
  --header "Content-Type: application/json" \
  --data '{
    "pageSize": 1000,
    "expands": ["asset.summary"],
    "query": {"criteria": {
      "portfolio": {"portfolioReferences": [{"portfolioTicker": "702T"}]},
      "dateTime": {"modifyTimeRange": {
        "startTime": "2026-07-14T16:00:00Z",
        "endTime": "2026-07-14T17:00:00Z"
      }}
    }}
  }'
```

BlackRock filter requests accept at most 100 portfolios and a modify-time range of at most 3,600 seconds. When `nextPageToken` is returned, resend the same filter with that token. The background refresher handles oversized portfolio groups automatically; the proxy does not expand them.

Read calls retry bounded `429`, `500`, `502`, `503`, and `504` responses and honor `Retry-After`. Write calls are not retried because their outcome may be unknown. Dynatrace records one structured event per BlackRock call with correlation, endpoint, status, duration, sanitized query, portfolio group/number, and BlackRock request IDs.

## Snowflake

When `TRADE_REFRESH_ENABLED=true`, the service creates:

| Object | Purpose |
| --- | --- |
| `<database>.STAGING_ALADDIN.TRADES` | One current row per allocation-level trade |
| `<database>.STAGING_ALADDIN.TRADE_REFRESH_STATE` | Checkpoint, lease, cached group membership, and recovery state |

Databases are `TCW_CORE_DEV` for local/sandbox/development, `TCW_CORE_QA` for QA, and `TCW_CORE` for production. `SNOWFLAKE_SCHEMA` defaults to `STAGING_ALADDIN` and accepts case-insensitive environment values.

`TRADES` has 216 columns covering all 200 OpenAPI trade-data leaves plus derived and operational fields. Key layout:

- `AS_OF_DATE DATE` is the first column and comes from `tradeDate`.
- `PORTFOLIO_NUMBER VARCHAR` is second and comes from `portfolioReference.portfolioTicker`.
- OpenAPI doubles use `FLOAT`, int32 values use `NUMBER(10,0)`, dates use `DATE`, and timestamps use `TIMESTAMP_TZ`.
- Repeated child properties use separate RFC 4180 comma-separated `VARCHAR` columns in source order.
- `SOURCE_DATA` retains the complete nested source for audit, relationships, and future fields.
- `BLOCK_CURRENT_KEY`, `BLOCK_VERSION_KEY`, `TRADE_CURRENT_KEY`, and `TRADE_VERSION_KEY` support lineage and idempotent MERGE. `TRADE_ID` is the final column.

All stored timestamps and application logs use `America/Los_Angeles`, including PST/PDT transitions. No Snowflake stage, staging table, or detail table is created.

## Refresh

- Runs on weekdays between `TRADE_REFRESH_START_TIME_PT` and `TRADE_REFRESH_END_TIME_PT` at `TRADE_REFRESH_INTERVAL_MINUTES`.
- Accepts `group:<ticker>`, one portfolio, or comma-separated portfolios through `ALADDIN_TRADE_PORTFOLIO_FILTER`.
- Resolves portfolio groups on the configured `ALADDIN_PORTFOLIO_GROUP_REFRESH_MINUTES` interval and persists the cache across restarts.
- Splits group members into parallel batches of at most 100 and follows every trade page token.
- Uses all OpenAPI trade enrichments, a safety delay, lookback overlap, deterministic MERGE, and a durable checkpoint.
- Replays missed windows after crashes, redeployments, weekends, rate limits, and retryable upstream failures.
- Caps first-run and outage recovery with `TRADE_REFRESH_MAX_CATCHUP_DURATION`. Use whole hours or days, such as `12H`, `48H`, or `2D`; one day is exactly 24 hours.
- Isolates removed or inactive portfolios and persists unresolved recovery windows without blocking active portfolios.
- Shares the OpenAPI limits of 1,000 reads and 250 writes per minute with proxy traffic.

## API Key

Every environment uses the Azure Key Vault secret `trade-refresher-api-key`. Generate a 256-bit value from Git Bash on Windows:

```sh
./scripts/generate-trade-api-key.sh development
```

The script prints the secret name and value but does not modify Azure Key Vault.

## Verify

```sh
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/trade-refresher
```
