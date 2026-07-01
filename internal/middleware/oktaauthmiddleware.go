package middleware

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"refresher/trade-refresher/internal/utils/auth"
	"refresher/trade-refresher/internal/utils/types"
)

// NewOktaAuthMiddleware function will create a custom Okta Authentication middleware.  It is recommended to customize this function based on
// the application requirements and source all environment variable values from the environment configurations.  It is recommended to execute this
// function from the private routes declaration section.
//
// Parameters:
//
// issuer - string : The TCW Okta issuer
//
// claimsToValidate - map[string]string : The unique key value pairs of the claims to be validated (typically audience and issuer)
//
// claimsToExtract - map[string]string : The unique key value pairs of the claims to be extracted and the request header context key to place extracted values
//
// environment - string : The runtime environment of the service
func NewOktaAuthMiddleware(issuer string, claimsToValidate map[string]string, claimsToExtract map[string]string, environment types.Environment) func(*fiber.Ctx) error {
	oc := auth.OktaConfig{
		Issuer:           issuer,
		ClaimsToValidate: claimsToValidate,
		ClaimsToExtract:  claimsToExtract,
		Environment:      environment.String(),
		Bypass: func(ctx *fiber.Ctx, environment string) (bool, error) {
			if environment == "" {
				return false, errors.New("invalid environment value provided")
			}
			switch environment {
			case "local":
				return false, nil
			case "test":
				return true, nil
			default:
				return false, nil
			}
		},
	}

	return auth.NewOktaAuthMiddleware(oc)
}
