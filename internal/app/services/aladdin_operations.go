package services

import (
	"net/http"
	"strings"
)

const (
	BlackRockRequestIDHeader       = "VND.com.blackrock.Request-ID"
	BlackRockOriginTimestampHeader = "VND.com.blackrock.Origin-Timestamp"
	BlackRockResponseIDHeader      = "VND.com.blackrock.Response-ID"
	BlackRockResponseTimeHeader    = "VND.com.blackrock.Response-Timestamp"
	AuthorizationHeader            = "Authorization"
	CorrelationIDHeader            = "X-Correlation-ID"
	RequestIDHeader                = "X-Request-ID"
)

type QuotaFamily string

const (
	QuotaRead  QuotaFamily = "read"
	QuotaWrite QuotaFamily = "write"
)

type TradeOperation struct {
	Name         string
	Method       string
	UpstreamPath string
	QuotaFamily  QuotaFamily
	QuotaLimit   int
	FromAPIBase  bool
}

func portfolioGroupMembersOperation(portfolioGroupPath string) TradeOperation {
	return TradeOperation{
		Name:         "PortfolioGroupAPI_GetMembers",
		Method:       http.MethodGet,
		UpstreamPath: strings.TrimRight(portfolioGroupPath, "/") + "/{portfolioGroup}/members",
		QuotaFamily:  QuotaRead,
		QuotaLimit:   maxAladdinReadsPerMinute,
		FromAPIBase:  true,
	}
}

var (
	OperationGetLongRunningOperation = TradeOperation{
		Name:         "TradeAPI_GetLongrunningOperation",
		Method:       http.MethodGet,
		UpstreamPath: "/longrunningoperations/{id}",
		QuotaFamily:  QuotaRead,
		QuotaLimit:   maxAladdinReadsPerMinute,
	}
	OperationFilterTrades = TradeOperation{
		Name:         "TradeAPI_FilterTrades",
		Method:       http.MethodPost,
		UpstreamPath: "/trades:filter",
		QuotaFamily:  QuotaRead,
		QuotaLimit:   maxAladdinReadsPerMinute,
	}
	OperationRetrieveTrades = TradeOperation{
		Name:         "TradeAPI_RetrieveTrades",
		Method:       http.MethodPost,
		UpstreamPath: "/trades:retrieve",
		QuotaFamily:  QuotaRead,
		QuotaLimit:   maxAladdinReadsPerMinute,
	}
	OperationPostTrade = TradeOperation{
		Name:         "TradeAPI_PostTrade",
		Method:       http.MethodPost,
		UpstreamPath: "/trades:post",
		QuotaFamily:  QuotaWrite,
		QuotaLimit:   maxAladdinWritesPerMinute,
	}
	OperationBatchPostTrades = TradeOperation{
		Name:         "TradeAPI_BatchPostTrades",
		Method:       http.MethodPost,
		UpstreamPath: "/trades:batchPost",
		QuotaFamily:  QuotaWrite,
		QuotaLimit:   maxAladdinWritesPerMinute,
	}
	OperationCancelTrade = TradeOperation{
		Name:         "TradeAPI_CancelTrade",
		Method:       http.MethodPost,
		UpstreamPath: "/trades:cancel",
		QuotaFamily:  QuotaWrite,
		QuotaLimit:   maxAladdinWritesPerMinute,
	}
	OperationBatchCancelTrades = TradeOperation{
		Name:         "TradeAPI_BatchCancelTrades",
		Method:       http.MethodPost,
		UpstreamPath: "/trades:batchCancel",
		QuotaFamily:  QuotaWrite,
		QuotaLimit:   maxAladdinWritesPerMinute,
	}
)
