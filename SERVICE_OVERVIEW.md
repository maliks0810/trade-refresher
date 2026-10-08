# trade-refresher — Service Overview

trade-refresher is a Go service with two jobs:

1. **Trade refresher.** A background job copies BlackRock Aladdin trades into Snowflake.
   - **When it runs:** weekdays, inside a Pacific-time window (04:00–18:00 by default), every 3 minutes by default.
   - **What it pulls:** trades modified in each time window, for the configured portfolios. The default is the portfolio group `TCW_ALL`. Group members are fetched in batches of up to 100, and every page of results is followed.
   - **Where it writes:** one current row per allocation-level trade in `<db>.STAGING_ALADDIN.TRADES`. Progress and recovery state go in `TRADE_REFRESH_STATE`.
   - **Recovery:** it saves a checkpoint after each window. After crashes, redeploys, weekends or upstream errors it replays the windows it missed, up to a set limit (7 days by default).
   - **Error reporting:** failures are reported to GEM.
   - **Off locally:** the job is disabled unless `TRADE_REFRESH_ENABLED=true`.
2. **Trade API proxy.** Services inside the same AKS cluster can call Aladdin's Trade API through it. It shares Aladdin's rate limits with the refresher (1,000 reads and 250 writes per minute) and retries reads but never writes.

All HTTP routes are on port **8100** under **`/de/v1/api`**.

## Trade API endpoints

Every request needs `Authorization: Bearer <trade-refresher-api-key>` and an `X-Correlation-ID` header. POST requests also need a JSON body.

| Endpoint | What it does |
| --- | --- |
| `POST /trades:filter` | Searches block trades by criteria, such as portfolios and a modify-time range. Aladdin allows at most 100 portfolios and a range of at most 1 hour per request. Returns pages of results with `nextPageToken`. |
| `POST /trades:retrieve` | Fetches specific trades by `tradeKeys`, `externTradeKeys` or `orderKeys`. |
| `POST /trades:post` | Creates or updates one block trade in Aladdin. |
| `POST /trades:batchPost` | Creates or updates many block trades. Returns a long-running operation to poll. |
| `POST /trades:cancel` | Cancels one trade by `tradeKey`. |
| `POST /trades:batchCancel` | Cancels many trades. Returns a long-running operation to poll. |
| `GET /longrunningoperations/{id}` | Returns the status of a batch post or batch cancel, including the result once it finishes. |

## Health checks (no auth)

| Endpoint | What it does |
| --- | --- |
| `GET /de/v1/api/live` | Liveness check; returns `{"status":"healthy"}`. |
| `GET /de/v1/api/ready` | Readiness check; returns the same response. |
| `GET /health` on port **8080** | Separate plain-text health check on its own server. |

## Documentation (no auth)

These are only served when running locally or when `INTERNAL_INGRESS_ENABLED=true`.

| Endpoint | What it does |
| --- | --- |
| `GET /de/v1/api/docs/` | Swagger UI for trying the trade endpoints. |
| `GET /de/v1/api/openapi.json` | The OpenAPI spec. |

## Access

None of these endpoints is public on the internet. The trade endpoints are meant for callers inside the cluster, at `http://trade-refresher.<namespace>.svc.cluster.local/de/v1/api`, or through the internal ingress when it's enabled. Only the health and docs endpoints work without the API key.
