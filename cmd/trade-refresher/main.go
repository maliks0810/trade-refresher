package main

import (
	"context"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"refresher/trade-refresher/configs"
	"refresher/trade-refresher/internal/app/handlers"
	"refresher/trade-refresher/internal/app/services"
	"refresher/trade-refresher/internal/middleware"
	"refresher/trade-refresher/internal/routes"
	"refresher/trade-refresher/internal/utils/azure"
	"refresher/trade-refresher/internal/utils/log"
	"refresher/trade-refresher/internal/utils/net"
)

func main() {
	configs.Load()

	app := fiber.New(fiber.Config{
		BodyLimit:   10 * 1024 * 1024,
		Concurrency: 256,
	})
	middleware.FiberMiddleware(app)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vault := newVault()
	limiter := services.NewSharedLimiter(services.RateLimitConfig{
		ReadPerMinute:  configs.EnvConfigs.AladdinReadMaxRequestsPerMinute,
		WritePerMinute: configs.EnvConfigs.AladdinWriteMaxRequestsPerMinute,
		ReadBurst:      configs.EnvConfigs.AladdinReadBurst,
		WriteBurst:     configs.EnvConfigs.AladdinWriteBurst,
	})
	aladdinClient := services.NewAladdinClient(aladdinConfig(), vault, limiter)
	apiKeyAuth := services.NewAPIKeyAuthenticator(
		vault,
		configs.EnvConfigs.KeyVaultTradeRefresherAPIKeyKey,
		5*time.Minute,
		"",
	)
	if err := apiKeyAuth.Warm(); err != nil {
		log.Logger.Fatal("api.auth.initialization_failed", zap.Error(err))
	}
	tradeHandler := handlers.NewTradeHandler(aladdinClient)

	store, err := newSnowflakeStore(ctx, vault)
	if err != nil {
		log.Logger.Fatal("snowflake.initialization_failed", zap.Error(err))
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			log.Logger.Error("snowflake.close_failed", zap.Error(closeErr))
		}
	}()

	gemClient := services.NewGemClient(configs.EnvConfigs.GEMURL, configs.EnvConfigs.GEMAPIEnabled)
	scheduler := services.NewTradeScheduler(schedulerConfig(), aladdinClient, store, gemClient)
	routes.UtilityRoutes(ctx, app)
	docsEnabled := configs.DocumentationEnabled()
	if err := routes.DocsRoutes(app, docsEnabled); err != nil {
		log.Logger.Fatal("api.docs.initialization_failed", zap.Error(err))
	}
	if !docsEnabled {
		log.Logger.Info("api.docs.disabled")
	}
	routes.TradeRoutes(app, tradeHandler, apiKeyAuth)
	schedulerDone := scheduler.Start(ctx)

	net.StartServerWithGracefulShutdown(app, cancel)
	cancel()
	select {
	case <-schedulerDone:
	case <-time.After(8 * time.Second):
		log.Logger.Warn("refresh.scheduler.shutdown_timeout")
	}
}

func newVault() azure.Vault {
	if strings.TrimSpace(configs.EnvConfigs.KeyVaultVelocityURL) == "" {
		return nil
	}
	return azure.NewVault(configs.EnvConfigs.KeyVaultVelocityURL, !strings.EqualFold(configs.EnvConfigs.GoEnvironment, "local"))
}

func aladdinConfig() services.AladdinClientConfig {
	return services.AladdinClientConfig{
		BaseURL:                configs.EnvConfigs.AladdinBaseURL,
		TradePath:              configs.EnvConfigs.AladdinTradePath,
		PortfolioGroupPath:     configs.EnvConfigs.AladdinPortfolioGroupPath,
		OAuthEnabled:           configs.EnvConfigs.AladdinOAuthEnabled,
		TokenURL:               configs.EnvConfigs.AladdinOAuthTokenURL,
		Scopes:                 splitScopes(configs.EnvConfigs.AladdinOAuthScopes),
		ClientIDSecretName:     configs.EnvConfigs.KeyVaultRefresherOAuthClientIDKey,
		ClientSecretSecretName: configs.EnvConfigs.KeyVaultRefresherOAuthClientSecretKey,
		CredentialsTTL:         time.Duration(configs.EnvConfigs.AladdinOAuthCredentialsTTLInMinutes) * time.Minute,
		Timeout:                time.Duration(configs.EnvConfigs.AladdinTimeoutMinutes) * time.Minute,
		RetryReadAttempts:      configs.EnvConfigs.AladdinRetryMaxAttemptsRead,
		RetryWriteAttempts:     configs.EnvConfigs.AladdinRetryMaxAttemptsWrite,
		RetryBaseDelay:         time.Duration(configs.EnvConfigs.AladdinRetryBaseMilliseconds) * time.Millisecond,
		RetryMaxDelay:          time.Duration(configs.EnvConfigs.AladdinRetryMaxMilliseconds) * time.Millisecond,
		APIEnabled:             configs.EnvConfigs.AladdinAPIEnabled,
	}
}

func newSnowflakeStore(ctx context.Context, vault azure.Vault) (*services.SnowflakeStore, error) {
	if !configs.EnvConfigs.TradeRefreshEnabled {
		return services.NewSnowflakeStoreWithDB(nil, services.SnowflakeConfig{}), nil
	}
	return services.NewSnowflakeStore(ctx, snowflakeConfig(), vault)
}

func snowflakeConfig() services.SnowflakeConfig {
	return services.SnowflakeConfig{
		Account:            configs.EnvConfigs.SnowflakeAccount,
		User:               configs.EnvConfigs.SnowflakeUser,
		Role:               configs.EnvConfigs.SnowflakeRole,
		Warehouse:          configs.EnvConfigs.SnowflakeWarehouse,
		Database:           configs.EnvConfigs.SnowflakeDatabase,
		Schema:             configs.EnvConfigs.SnowflakeSchema,
		Authenticator:      configs.EnvConfigs.SnowflakeAuthenticator,
		KeepSessionAlive:   configs.EnvConfigs.SnowflakeKeepSessionAlive,
		DerSecretName:      configs.EnvConfigs.KeyVaultSnowflakeDERKey,
		PasswordSecretName: configs.EnvConfigs.KeyVaultSnowflakePasswordKey,
		TradeTable:         "TRADES",
		StateTable:         "TRADE_REFRESH_STATE",
		InsertBatchSize:    configs.EnvConfigs.SnowflakeInsertBatchSize,
		MaxOpenConns:       configs.EnvConfigs.SnowflakeMaxOpenConns,
		MaxIdleConns:       configs.EnvConfigs.SnowflakeMaxIdleConns,
		ConnectionTTL:      time.Duration(configs.EnvConfigs.SnowflakeConnectionTTLInMinutes) * time.Minute,
	}
}

func schedulerConfig() services.SchedulerConfig {
	if !configs.EnvConfigs.TradeRefreshEnabled {
		return services.SchedulerConfig{Enabled: false}
	}
	portfolioFilter, err := services.ParsePortfolioFilter(configs.TradePortfolioFilterValue())
	if err != nil {
		log.Logger.Fatal("refresh.portfolio_filter.invalid", zap.Error(err))
	}
	businessStartMinute, businessEndMinute := configs.TradeRefreshWindowMinutes()
	return services.SchedulerConfig{
		Enabled:                       configs.EnvConfigs.TradeRefreshEnabled,
		Interval:                      time.Duration(configs.EnvConfigs.TradeRefreshIntervalMinutes) * time.Minute,
		PortfolioFilter:               portfolioFilter,
		PortfolioGroupRefreshInterval: time.Duration(configs.EnvConfigs.AladdinPortfolioGroupRefreshMinutes) * time.Minute,
		PageSize:                      configs.EnvConfigs.AladdinTradePageSize,
		Lookback:                      time.Duration(configs.EnvConfigs.TradeRefreshLookbackSeconds) * time.Second,
		SafetyDelay:                   time.Duration(configs.EnvConfigs.TradeRefreshSafetyDelaySeconds) * time.Second,
		BusinessStartMinutePT:         businessStartMinute,
		BusinessEndMinutePT:           businessEndMinute,
		MaxCatchup:                    configs.TradeRefreshMaxCatchupDuration(),
		LockTTL:                       time.Duration(configs.EnvConfigs.TradeRefreshLockTTLSeconds) * time.Second,
		DecodeWorkers:                 configs.EnvConfigs.TradeDecodeWorkers,
		GemReportAfterFailures:        configs.EnvConfigs.GEMErrorRetryLimitBeforeReport,
	}
}

func splitScopes(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\n' || r == '\t'
	})
	scopes := make([]string, 0, len(fields))
	for _, field := range fields {
		if trimmed := strings.TrimSpace(field); trimmed != "" {
			scopes = append(scopes, trimmed)
		}
	}
	return scopes
}
