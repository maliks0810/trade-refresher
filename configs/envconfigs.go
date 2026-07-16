package configs

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"

	"refresher/trade-refresher/internal/utils/log"
)

type envConfigs struct {
	GoEnvironment          string `mapstructure:"GOLANG_ENVIRONMENT"`
	InternalIngressEnabled bool   `mapstructure:"INTERNAL_INGRESS_ENABLED"`

	KeyVaultVelocityURL                   string `mapstructure:"AZ_KEY_VAULT_VELOCITY_URL"`
	KeyVaultSnowflakeDERKey               string `mapstructure:"AZ_SF_DEF_KEY"`
	KeyVaultSnowflakePasswordKey          string `mapstructure:"AZ_SF_PWD_KEY"`
	KeyVaultRefresherOAuthClientIDKey     string `mapstructure:"AZ_REFRESHER_OAUTH_CLIENT_ID_KEY"`
	KeyVaultRefresherOAuthClientSecretKey string `mapstructure:"AZ_REFRESHER_OAUTH_CLIENT_SECRET_KEY"`
	KeyVaultTradeRefresherAPIKeyKey       string `mapstructure:"AZ_TRADE_REFRESHER_API_KEY_KEY"`

	GEMURL                         string `mapstructure:"GEM_URL"`
	GEMAPIEnabled                  bool   `mapstructure:"GEM_API_ENABLED"`
	GEMErrorRetryLimitBeforeReport int    `mapstructure:"GEM_ERROR_RETRY_LIMIT_BEFORE_REPORT"`

	AladdinBaseURL                      string `mapstructure:"ALADDIN_BASE_URL"`
	AladdinOAuthEnabled                 bool   `mapstructure:"ALADDIN_OAUTH_ENABLED"`
	AladdinOAuthTokenURL                string `mapstructure:"ALADDIN_OAUTH_TOKEN_URL"`
	AladdinOAuthScopes                  string `mapstructure:"ALADDIN_OAUTH_SCOPES"`
	AladdinOAuthCredentialsTTLInMinutes int    `mapstructure:"ALADDIN_OAUTH_CREDENTIALS_TTL_IN_MINUTES"`
	AladdinTradePath                    string `mapstructure:"ALADDIN_TRADE_PATH"`
	AladdinPortfolioGroupPath           string `mapstructure:"ALADDIN_PORTFOLIO_GROUP_PATH"`
	AladdinPortfolioGroupRefreshMinutes int    `mapstructure:"ALADDIN_PORTFOLIO_GROUP_REFRESH_MINUTES"`
	AladdinTradePortfolioFilter         string `mapstructure:"ALADDIN_TRADE_PORTFOLIO_FILTER"`
	AladdinTradePageSize                int    `mapstructure:"ALADDIN_TRADE_PAGE_SIZE"`
	AladdinTimeoutMinutes               int    `mapstructure:"ALADDIN_TIMEOUT_MIN"`
	AladdinAPIEnabled                   bool   `mapstructure:"ALADDIN_API_ENABLED"`
	AladdinReadMaxRequestsPerMinute     int    `mapstructure:"ALADDIN_READ_MAX_REQUESTS_PER_MIN"`
	AladdinWriteMaxRequestsPerMinute    int    `mapstructure:"ALADDIN_WRITE_MAX_REQUESTS_PER_MIN"`
	AladdinReadBurst                    int    `mapstructure:"ALADDIN_READ_BURST"`
	AladdinWriteBurst                   int    `mapstructure:"ALADDIN_WRITE_BURST"`
	AladdinRetryMaxAttemptsRead         int    `mapstructure:"ALADDIN_RETRY_MAX_ATTEMPTS_READ"`
	AladdinRetryMaxAttemptsWrite        int    `mapstructure:"ALADDIN_RETRY_MAX_ATTEMPTS_WRITE"`
	AladdinRetryBaseMilliseconds        int    `mapstructure:"ALADDIN_RETRY_BASE_MS"`
	AladdinRetryMaxMilliseconds         int    `mapstructure:"ALADDIN_RETRY_MAX_MS"`

	SnowflakeAccount                string `mapstructure:"SNOWFLAKE_ACCOUNT"`
	SnowflakeUser                   string `mapstructure:"SNOWFLAKE_USER"`
	SnowflakeRole                   string `mapstructure:"SNOWFLAKE_ROLE"`
	SnowflakeWarehouse              string `mapstructure:"SNOWFLAKE_WAREHOUSE"`
	SnowflakeDatabase               string `mapstructure:"SNOWFLAKE_DATABASE"`
	SnowflakeSchema                 string `mapstructure:"SNOWFLAKE_SCHEMA"`
	SnowflakeAuthenticator          string `mapstructure:"SNOWFLAKE_AUTHENTICATOR"`
	SnowflakeKeepSessionAlive       bool   `mapstructure:"SNOWFLAKE_KEEP_SESSION_ALIVE"`
	SnowflakeConnectionTTLInMinutes int    `mapstructure:"SNOWFLAKE_CONNECTION_TTL_IN_MINUTES"`
	SnowflakeInsertBatchSize        int    `mapstructure:"SNOWFLAKE_INSERT_BATCH_SIZE"`
	SnowflakeMaxOpenConns           int    `mapstructure:"SNOWFLAKE_MAX_OPEN_CONNS"`
	SnowflakeMaxIdleConns           int    `mapstructure:"SNOWFLAKE_MAX_IDLE_CONNS"`

	TradeRefreshIntervalMinutes    int    `mapstructure:"TRADE_REFRESH_INTERVAL_MINUTES"`
	TradeRefreshEnabled            bool   `mapstructure:"TRADE_REFRESH_ENABLED"`
	TradeRefreshStartTimePT        string `mapstructure:"TRADE_REFRESH_START_TIME_PT"`
	TradeRefreshEndTimePT          string `mapstructure:"TRADE_REFRESH_END_TIME_PT"`
	TradeRefreshLookbackSeconds    int    `mapstructure:"TRADE_REFRESH_LOOKBACK_SECONDS"`
	TradeRefreshSafetyDelaySeconds int    `mapstructure:"TRADE_REFRESH_SAFETY_DELAY_SECONDS"`
	TradeRefreshMaxCatchupDuration string `mapstructure:"TRADE_REFRESH_MAX_CATCHUP_DURATION"`
	TradeRefreshLockTTLSeconds     int    `mapstructure:"TRADE_REFRESH_LOCK_TTL_SECS"`
	TradeDecodeWorkers             int    `mapstructure:"TRADE_DECODE_WORKERS"`
}

var EnvConfigs *envConfigs

func Load() {
	EnvConfigs = loadEnvironmentVariables()
}

func loadEnvironmentVariables() (configs *envConfigs) {
	viper.AddConfigPath(".")
	viper.AddConfigPath("./env")
	viper.AddConfigPath("/env")
	viper.AddConfigPath("../../env")
	viper.AddConfigPath("/go/bin/env")
	viper.SetConfigType("env")

	viper.SetDefault("GOLANG_ENVIRONMENT", "local")
	viper.SetDefault("INTERNAL_INGRESS_ENABLED", false)
	viper.SetDefault("ALADDIN_BASE_URL", "https://tcw.blackrock.com/api")
	viper.SetDefault("ALADDIN_OAUTH_ENABLED", true)
	viper.SetDefault("ALADDIN_OAUTH_TOKEN_URL", "https://tcw.blackrock.com/api/oauth2/default/v1/token")
	viper.SetDefault("ALADDIN_OAUTH_SCOPES", "trading.trade_processing.trade.v2.Trade:read,trading.trade_processing.trade.v2.Trade:write,portfolio.configuration.portfolio_group.v1.PortfolioGroup:read")
	viper.SetDefault("ALADDIN_TRADE_PATH", "/trading/trade-processing/trade/v2")
	viper.SetDefault("ALADDIN_PORTFOLIO_GROUP_PATH", "/portfolio/configuration/portfolio-group/v1/portfolioGroups")
	viper.SetDefault("ALADDIN_PORTFOLIO_GROUP_REFRESH_MINUTES", 60)
	viper.SetDefault("ALADDIN_TRADE_PORTFOLIO_FILTER", "group:TCW_ALL")
	viper.SetDefault("ALADDIN_TRADE_PAGE_SIZE", 1000)
	viper.SetDefault("ALADDIN_TIMEOUT_MIN", 5)
	viper.SetDefault("ALADDIN_API_ENABLED", true)
	viper.SetDefault("ALADDIN_READ_MAX_REQUESTS_PER_MIN", 1000)
	viper.SetDefault("ALADDIN_WRITE_MAX_REQUESTS_PER_MIN", 250)
	viper.SetDefault("ALADDIN_READ_BURST", 25)
	viper.SetDefault("ALADDIN_WRITE_BURST", 5)
	viper.SetDefault("ALADDIN_RETRY_MAX_ATTEMPTS_READ", 6)
	viper.SetDefault("ALADDIN_RETRY_MAX_ATTEMPTS_WRITE", 1)
	viper.SetDefault("ALADDIN_RETRY_BASE_MS", 500)
	viper.SetDefault("ALADDIN_RETRY_MAX_MS", 30000)
	viper.SetDefault("ALADDIN_OAUTH_CREDENTIALS_TTL_IN_MINUTES", 480)
	viper.SetDefault("SNOWFLAKE_SCHEMA", "STAGING_ALADDIN")
	viper.SetDefault("SNOWFLAKE_KEEP_SESSION_ALIVE", true)
	viper.SetDefault("SNOWFLAKE_CONNECTION_TTL_IN_MINUTES", 480)
	viper.SetDefault("SNOWFLAKE_INSERT_BATCH_SIZE", 5000)
	viper.SetDefault("SNOWFLAKE_MAX_OPEN_CONNS", 4)
	viper.SetDefault("SNOWFLAKE_MAX_IDLE_CONNS", 2)
	viper.SetDefault("TRADE_REFRESH_INTERVAL_MINUTES", 3)
	viper.SetDefault("TRADE_REFRESH_ENABLED", false)
	viper.SetDefault("TRADE_REFRESH_START_TIME_PT", "04:00")
	viper.SetDefault("TRADE_REFRESH_END_TIME_PT", "18:00")
	viper.SetDefault("TRADE_REFRESH_LOOKBACK_SECONDS", 300)
	viper.SetDefault("TRADE_REFRESH_SAFETY_DELAY_SECONDS", 60)
	viper.SetDefault("TRADE_REFRESH_MAX_CATCHUP_DURATION", "7D")
	viper.SetDefault("TRADE_REFRESH_LOCK_TTL_SECS", 600)
	viper.SetDefault("TRADE_DECODE_WORKERS", 8)

	viper.BindEnv("GOLANG_ENVIRONMENT")
	bindTradeEnv()

	goEnvironment := viper.GetString("GOLANG_ENVIRONMENT")

	envFile := ".env." + goEnvironment
	viper.SetConfigName(envFile)

	if err := viper.ReadInConfig(); err != nil {
		log.Logger.Fatal("Unable to load environment configuration file" + err.Error())
	}

	if err := viper.Unmarshal(&configs); err != nil {
		log.Logger.Fatal(err.Error())
	}

	configs.GoEnvironment = goEnvironment
	configs.SnowflakeSchema = normalizeSnowflakeSchema(configs.SnowflakeSchema)
	validateOrFatal(configs)

	return
}

func normalizeSnowflakeSchema(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func bindTradeEnv() {
	for _, key := range []string{
		"INTERNAL_INGRESS_ENABLED",
		"AZ_KEY_VAULT_VELOCITY_URL",
		"AZ_SF_DEF_KEY",
		"AZ_SF_PWD_KEY",
		"AZ_REFRESHER_OAUTH_CLIENT_ID_KEY",
		"AZ_REFRESHER_OAUTH_CLIENT_SECRET_KEY",
		"AZ_TRADE_REFRESHER_API_KEY_KEY",
		"GEM_URL",
		"GEM_API_ENABLED",
		"GEM_ERROR_RETRY_LIMIT_BEFORE_REPORT",
		"ALADDIN_BASE_URL",
		"ALADDIN_OAUTH_ENABLED",
		"ALADDIN_OAUTH_TOKEN_URL",
		"ALADDIN_OAUTH_SCOPES",
		"ALADDIN_OAUTH_CREDENTIALS_TTL_IN_MINUTES",
		"ALADDIN_TRADE_PATH",
		"ALADDIN_PORTFOLIO_GROUP_PATH",
		"ALADDIN_PORTFOLIO_GROUP_REFRESH_MINUTES",
		"ALADDIN_TRADE_PORTFOLIO_FILTER",
		"ALADDIN_TRADE_PAGE_SIZE",
		"ALADDIN_TIMEOUT_MIN",
		"ALADDIN_API_ENABLED",
		"ALADDIN_READ_MAX_REQUESTS_PER_MIN",
		"ALADDIN_WRITE_MAX_REQUESTS_PER_MIN",
		"ALADDIN_READ_BURST",
		"ALADDIN_WRITE_BURST",
		"ALADDIN_RETRY_MAX_ATTEMPTS_READ",
		"ALADDIN_RETRY_MAX_ATTEMPTS_WRITE",
		"ALADDIN_RETRY_BASE_MS",
		"ALADDIN_RETRY_MAX_MS",
		"SNOWFLAKE_ACCOUNT",
		"SNOWFLAKE_USER",
		"SNOWFLAKE_ROLE",
		"SNOWFLAKE_WAREHOUSE",
		"SNOWFLAKE_DATABASE",
		"SNOWFLAKE_SCHEMA",
		"SNOWFLAKE_AUTHENTICATOR",
		"SNOWFLAKE_KEEP_SESSION_ALIVE",
		"SNOWFLAKE_CONNECTION_TTL_IN_MINUTES",
		"SNOWFLAKE_INSERT_BATCH_SIZE",
		"SNOWFLAKE_MAX_OPEN_CONNS",
		"SNOWFLAKE_MAX_IDLE_CONNS",
		"TRADE_REFRESH_ENABLED",
		"TRADE_REFRESH_INTERVAL_MINUTES",
		"TRADE_REFRESH_START_TIME_PT",
		"TRADE_REFRESH_END_TIME_PT",
		"TRADE_REFRESH_LOOKBACK_SECONDS",
		"TRADE_REFRESH_SAFETY_DELAY_SECONDS",
		"TRADE_REFRESH_MAX_CATCHUP_DURATION",
		"TRADE_REFRESH_LOCK_TTL_SECS",
		"TRADE_DECODE_WORKERS",
	} {
		if err := viper.BindEnv(key); err != nil {
			log.Logger.Fatal(fmt.Sprintf("unable to bind environment key %s: %v", key, err))
		}
	}
}

func DocumentationEnabled() bool {
	if EnvConfigs == nil {
		return false
	}
	return strings.EqualFold(EnvConfigs.GoEnvironment, "local") || EnvConfigs.InternalIngressEnabled
}

func validateOrFatal(configs *envConfigs) {
	if configs == nil {
		log.Logger.Fatal("environment configuration cannot be nil")
	}
	if strings.TrimSpace(configs.SnowflakeSchema) == "" {
		log.Logger.Fatal("SNOWFLAKE_SCHEMA cannot be empty")
	}
	expectedDatabase := expectedSnowflakeDatabase(configs.GoEnvironment)
	if expectedDatabase != "" && configs.SnowflakeDatabase != "" && configs.SnowflakeDatabase != expectedDatabase {
		log.Logger.Fatal(fmt.Sprintf("invalid Snowflake database %q for environment %q: expected %q", configs.SnowflakeDatabase, configs.GoEnvironment, expectedDatabase))
	}
	if configs.TradeRefreshEnabled && configs.TradeRefreshIntervalMinutes < 1 {
		log.Logger.Fatal("TRADE_REFRESH_INTERVAL_MINUTES must be at least 1 when trade refresh is enabled")
	}
	if configs.AladdinTradePageSize < 1 {
		log.Logger.Fatal("ALADDIN_TRADE_PAGE_SIZE must be at least 1")
	}
	if configs.AladdinPortfolioGroupRefreshMinutes < 1 || configs.AladdinPortfolioGroupRefreshMinutes > 24*60 {
		log.Logger.Fatal("ALADDIN_PORTFOLIO_GROUP_REFRESH_MINUTES must be between 1 and 1440")
	}
	if configs.AladdinReadMaxRequestsPerMinute < 1 || configs.AladdinReadMaxRequestsPerMinute > 1000 {
		log.Logger.Fatal("ALADDIN_READ_MAX_REQUESTS_PER_MIN must be between 1 and the OpenAPI limit of 1000")
	}
	if configs.AladdinWriteMaxRequestsPerMinute < 1 || configs.AladdinWriteMaxRequestsPerMinute > 250 {
		log.Logger.Fatal("ALADDIN_WRITE_MAX_REQUESTS_PER_MIN must be between 1 and the OpenAPI limit of 250")
	}
	if configs.AladdinReadBurst < 1 || configs.AladdinReadBurst > configs.AladdinReadMaxRequestsPerMinute {
		log.Logger.Fatal("ALADDIN_READ_BURST must be between 1 and ALADDIN_READ_MAX_REQUESTS_PER_MIN")
	}
	if configs.AladdinWriteBurst < 1 || configs.AladdinWriteBurst > configs.AladdinWriteMaxRequestsPerMinute {
		log.Logger.Fatal("ALADDIN_WRITE_BURST must be between 1 and ALADDIN_WRITE_MAX_REQUESTS_PER_MIN")
	}
	if configs.TradeRefreshEnabled {
		startMinute, startErr := parseTimeOfDay(configs.TradeRefreshStartTimePT)
		endMinute, endErr := parseTimeOfDay(configs.TradeRefreshEndTimePT)
		if startErr != nil || endErr != nil || startMinute >= endMinute {
			log.Logger.Fatal("TRADE_REFRESH_START_TIME_PT and TRADE_REFRESH_END_TIME_PT must use HH:MM and define an increasing Pacific-time window")
		}
		if configs.TradeRefreshLookbackSeconds < 0 || configs.TradeRefreshSafetyDelaySeconds < 0 {
			log.Logger.Fatal("trade refresh lookback and safety delay cannot be negative")
		}
		if _, err := parseCatchupDuration(configs.TradeRefreshMaxCatchupDuration); err != nil {
			log.Logger.Fatal(err.Error())
		}
		if configs.TradeRefreshLockTTLSeconds < 30 {
			log.Logger.Fatal("TRADE_REFRESH_LOCK_TTL_SECS must be at least 30")
		}
		if configs.TradeDecodeWorkers < 1 {
			log.Logger.Fatal("TRADE_DECODE_WORKERS must be at least 1")
		}
		if configs.SnowflakeInsertBatchSize < 1 || configs.SnowflakeInsertBatchSize > 100000 {
			log.Logger.Fatal("SNOWFLAKE_INSERT_BATCH_SIZE must be between 1 and 100000")
		}
	}
}

func TradeRefreshMaxCatchupDuration() time.Duration {
	duration, _ := parseCatchupDuration(EnvConfigs.TradeRefreshMaxCatchupDuration)
	return duration
}

func parseCatchupDuration(value string) (time.Duration, error) {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	if len(normalized) < 2 {
		return 0, fmt.Errorf("TRADE_REFRESH_MAX_CATCHUP_DURATION must use a positive whole number followed by H or D")
	}
	var unit time.Duration
	switch normalized[len(normalized)-1] {
	case 'H':
		unit = time.Hour
	case 'D':
		unit = 24 * time.Hour
	default:
		return 0, fmt.Errorf("TRADE_REFRESH_MAX_CATCHUP_DURATION %q must end in H or D", value)
	}
	amount, err := strconv.ParseInt(normalized[:len(normalized)-1], 10, 64)
	if err != nil || amount < 1 {
		return 0, fmt.Errorf("TRADE_REFRESH_MAX_CATCHUP_DURATION %q must contain a positive whole number", value)
	}
	const maximum = 365 * 24 * time.Hour
	if amount > int64(maximum/unit) {
		return 0, fmt.Errorf("TRADE_REFRESH_MAX_CATCHUP_DURATION %q exceeds the 365-day safety limit", value)
	}
	return time.Duration(amount) * unit, nil
}

func TradeRefreshWindowMinutes() (int, int) {
	start, _ := parseTimeOfDay(EnvConfigs.TradeRefreshStartTimePT)
	end, _ := parseTimeOfDay(EnvConfigs.TradeRefreshEndTimePT)
	return start, end
}

func parseTimeOfDay(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "24:00" {
		return 24 * 60, nil
	}
	parsed, err := time.Parse("15:04", value)
	if err != nil {
		return 0, err
	}
	return parsed.Hour()*60 + parsed.Minute(), nil
}

func expectedSnowflakeDatabase(environment string) string {
	switch strings.ToLower(environment) {
	case "local", "sandbox", "development", "dev":
		return "TCW_CORE_DEV"
	case "qa":
		return "TCW_CORE_QA"
	case "production", "prod":
		return "TCW_CORE"
	default:
		return ""
	}
}

func TradePortfolioFilterValue() string {
	if EnvConfigs == nil {
		return ""
	}
	if value := strings.TrimSpace(EnvConfigs.AladdinTradePortfolioFilter); value != "" {
		return value
	}
	return ""
}
