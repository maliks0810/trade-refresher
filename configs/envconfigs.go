package configs

import (
	"github.com/spf13/viper"

	"refresher/trade-refresher/internal/utils/log"
	"refresher/trade-refresher/internal/utils/types"
)

type envConfigs struct {
	GolangEnvironment			types.Environment
	HostEnvironment 			string 		`mapstructure:"HOST_ENVIRONMENT"`
	AuthAudience				string 		`mapstructure:"TCW_OKTA_AUDIENCE"`
	AuthIssuer					string 		`mapstructure:"TCW_OKTA_ISSUER"`
	AuthorizationUrl			string 		`mapstructure:"PERMITIO_AUTH_URL"`
	AuthorizationKey			string 		`mapstructure:"PERMITIO_AUTH_KEY"`
	AmqpConnection	 			string 		`mapstructure:"AMQP_CONNECTION_STRING"`
}

var EnvConfigs *envConfigs

func Load() {
	EnvConfigs = loadEnvironmentVariables()
}

func loadEnvironmentVariables() (configs *envConfigs) {
	viper.AddConfigPath(".")
	viper.AddConfigPath("/env")
	viper.AddConfigPath("../../env")
	viper.AddConfigPath("/go/bin/env")
	viper.SetConfigType("env")

	viper.SetDefault("GOLANG_ENVIRONMENT", "local")
	viper.SetDefault("TCW_OKTA_AUDIENCE", "api://default")
	viper.SetDefault("TCW_OKTA_ISSUER", "https://tcw.okta.com/oauth2/default")
	
	// General Configurations
	viper.BindEnv("GOLANG_ENVIRONMENT")

	// Authentication Configurations
	viper.BindEnv("TCW_OKTA_AUDIENCE")
	viper.BindEnv("TCW_OKTA_ISSUER")
	
	// Permit.IO Configurations
	viper.BindEnv("PERMITIO_AUTH_URL")
	viper.BindEnv("PERMITIO_AUTH_KEY")

	// AMQP Configurations (Injected by ES-PlatformEngineering)
	viper.BindEnv("AMQP_CONNECTION_STRING")

	golangEnv := viper.GetString("GOLANG_ENVIRONMENT")

	envFile := ".env." + golangEnv
	viper.SetConfigName(envFile)

	if err := viper.ReadInConfig(); err != nil {
		log.Logger.Fatal("Unable to load environment configuration file" + err.Error())
	}

	if err := viper.Unmarshal(&configs); err != nil {
		log.Logger.Fatal(err.Error())
	}

	configs.GolangEnvironment = types.Environment(golangEnv)

	return
}
