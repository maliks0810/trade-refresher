package configs

import (
	"bufio"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestEnvironmentFilesUseSupportedConfiguration(t *testing.T) {
	files, err := filepath.Glob("../env/.env.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 5 {
		t.Fatalf("environment file count = %d, want 5", len(files))
	}

	supported := supportedEnvironmentKeys()
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			values := readEnvFile(t, file)
			for key := range supported {
				if _, ok := values[key]; !ok {
					t.Fatalf("environment key %s is missing", key)
				}
			}
			for key := range values {
				if !supported[key] {
					t.Fatalf("unsupported environment key %s", key)
				}
			}

			v := viper.New()
			v.SetConfigFile(file)
			v.SetConfigType("env")
			if err := v.ReadInConfig(); err != nil {
				t.Fatal(err)
			}
			var config envConfigs
			if err := v.Unmarshal(&config); err != nil {
				t.Fatal(err)
			}
			config.GoEnvironment = v.GetString("GOLANG_ENVIRONMENT")

			if config.KeyVaultTradeRefresherAPIKeyKey != "trade-refresher-api-key" {
				t.Fatalf("API key secret = %q", config.KeyVaultTradeRefresherAPIKeyKey)
			}
			if config.GoEnvironment != "local" && config.InternalIngressEnabled {
				t.Fatal("AKS environment enables internal ingress documentation by default")
			}
			if config.SnowflakeSchema != "STAGING_ALADDIN" {
				t.Fatalf("Snowflake schema = %q, want STAGING_ALADDIN", config.SnowflakeSchema)
			}
			if want := expectedSnowflakeDatabase(config.GoEnvironment); config.SnowflakeDatabase != want {
				t.Fatalf("Snowflake database = %q, want %q", config.SnowflakeDatabase, want)
			}
			if config.GoEnvironment == "local" {
				if config.SnowflakeWarehouse != "COMPUTE_WH" {
					t.Fatalf("local Snowflake warehouse = %q, want COMPUTE_WH", config.SnowflakeWarehouse)
				}
			} else if config.SnowflakeWarehouse != "" {
				t.Fatalf("%s Snowflake warehouse = %q, want reference-refresher default", config.GoEnvironment, config.SnowflakeWarehouse)
			}
			if config.AladdinTradePath != "/trading/trade-processing/trade/v2" || config.AladdinPortfolioGroupPath != "/portfolio/configuration/portfolio-group/v1/portfolioGroups" || !strings.Contains(config.AladdinOAuthScopes, "Trade:read") || !strings.Contains(config.AladdinOAuthScopes, "Trade:write") || !strings.Contains(config.AladdinOAuthScopes, "PortfolioGroup:read") {
				t.Fatal("Aladdin paths or OAuth scopes are incomplete")
			}
			if config.AladdinPortfolioGroupRefreshMinutes < 1 || config.AladdinPortfolioGroupRefreshMinutes > 24*60 {
				t.Fatal("portfolio group refresh interval must be between one minute and one day")
			}
			if config.AladdinRetryMaxAttemptsRead < 1 || config.AladdinRetryMaxAttemptsWrite != 1 {
				t.Fatal("Aladdin retry attempts are invalid")
			}
			interval, err := strconv.Atoi(values["TRADE_REFRESH_INTERVAL_MINUTES"])
			if err != nil || interval < 1 {
				t.Fatalf("invalid refresh interval %q", values["TRADE_REFRESH_INTERVAL_MINUTES"])
			}
			if config.TradeRefreshIntervalMinutes != interval {
				t.Fatalf("refresh interval = %d, want %d", config.TradeRefreshIntervalMinutes, interval)
			}
			enabled, err := strconv.ParseBool(values["TRADE_REFRESH_ENABLED"])
			if err != nil {
				t.Fatalf("invalid TRADE_REFRESH_ENABLED %q", values["TRADE_REFRESH_ENABLED"])
			}
			if config.TradeRefreshEnabled != enabled {
				t.Fatalf("refresh enabled = %t, want %t", config.TradeRefreshEnabled, enabled)
			}
			startMinute, startErr := parseTimeOfDay(config.TradeRefreshStartTimePT)
			endMinute, endErr := parseTimeOfDay(config.TradeRefreshEndTimePT)
			if startErr != nil || endErr != nil || startMinute >= endMinute {
				t.Fatalf("invalid Pacific refresh window %q-%q", config.TradeRefreshStartTimePT, config.TradeRefreshEndTimePT)
			}
			if _, err := parseCatchupDuration(config.TradeRefreshMaxCatchupDuration); err != nil {
				t.Fatalf("invalid catch-up duration %q: %v", config.TradeRefreshMaxCatchupDuration, err)
			}
			if enabled && (config.SnowflakeUser == "" || config.SnowflakeRole == "" || config.SnowflakeAuthenticator == "") {
				t.Fatal("enabled refresh requires Snowflake user, role, and authenticator")
			}
		})
	}
}

func supportedEnvironmentKeys() map[string]bool {
	keys := make(map[string]bool)
	typeOfConfig := reflect.TypeOf(envConfigs{})
	for index := range typeOfConfig.NumField() {
		if key := typeOfConfig.Field(index).Tag.Get("mapstructure"); key != "" {
			keys[strings.ToUpper(key)] = true
		}
	}
	return keys
}

func readEnvFile(t *testing.T, path string) map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found || strings.TrimSpace(key) == "" {
			t.Fatalf("invalid environment line %q", line)
		}
		key = strings.TrimSpace(key)
		if _, exists := values[key]; exists {
			t.Fatalf("duplicate environment key %s", key)
		}
		values[key] = strings.TrimSpace(value)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func TestTradeRefreshEnabledAcceptsAKSEnvironmentName(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("TRADE_REFRESH_ENABLED", "true")
	bindTradeEnv()
	if !viper.GetBool("TRADE_REFRESH_ENABLED") {
		t.Fatal("TRADE_REFRESH_ENABLED was not bound")
	}
}

func TestSnowflakeSchemaIsCaseInsensitive(t *testing.T) {
	if got := normalizeSnowflakeSchema(" staging_aladdin "); got != "STAGING_ALADDIN" {
		t.Fatalf("normalized schema = %q, want STAGING_ALADDIN", got)
	}
}

func TestParseCatchupDuration(t *testing.T) {
	tests := map[string]time.Duration{
		"12H": 12 * time.Hour,
		"1D":  24 * time.Hour,
		"48H": 48 * time.Hour,
		"2d":  48 * time.Hour,
	}
	for value, want := range tests {
		got, err := parseCatchupDuration(value)
		if err != nil || got != want {
			t.Fatalf("parseCatchupDuration(%q) = %s, %v; want %s", value, got, err, want)
		}
	}
	for _, value := range []string{"", "0H", "1.5D", "24", "1W", "8761H", "366D"} {
		if _, err := parseCatchupDuration(value); err == nil {
			t.Fatalf("parseCatchupDuration(%q) returned nil error", value)
		}
	}
}

func TestDocumentationEnabled(t *testing.T) {
	previous := EnvConfigs
	t.Cleanup(func() { EnvConfigs = previous })

	tests := []struct {
		name        string
		environment string
		ingress     bool
		want        bool
	}{
		{name: "local without ingress", environment: "local", want: true},
		{name: "AKS without ingress", environment: "development", want: false},
		{name: "AKS with internal ingress", environment: "production", ingress: true, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			EnvConfigs = &envConfigs{GoEnvironment: test.environment, InternalIngressEnabled: test.ingress}
			if got := DocumentationEnabled(); got != test.want {
				t.Fatalf("DocumentationEnabled() = %t, want %t", got, test.want)
			}
		})
	}
}
