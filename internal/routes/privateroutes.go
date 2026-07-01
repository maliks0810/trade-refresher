package routes

import (
	"github.com/gofiber/fiber/v2"
	
	"refresher/trade-refresher/configs"
	"refresher/trade-refresher/internal/middleware"
)

type Handlers struct {
	GetIdentity func(*fiber.Ctx) error
}

// PrivateRoutes will add routes to the Fiber service that are guarded by some form of authentication.  Currently, the Platform Engineering
// Go/Fiber template supports the following authentication patterns:
//
// Okta Authentication: A JWT is verified against the TCW Okta issuer
//
// It is recommended to ensure all routes configured by PrivateRoutes should contain some form of authentication
//
// Parameters:
//
// app - *fiber.App : The Fiber application instance
//
// handlers - Handlers : A struct holding references to specific handlers - useful to override specific handlers with simulator handlers
func PrivateRoutes(app *fiber.App, handlers Handlers) {
	oktaAuthentication := func() func(*fiber.Ctx) error {
		return middleware.NewOktaAuthMiddleware(
		configs.EnvConfigs.AuthIssuer,
		map[string]string{
			"aud": configs.EnvConfigs.AuthAudience,
			"iss": configs.EnvConfigs.AuthIssuer,
		},
		map[string]string{
			"ad_samaccountname": "X-Authenticated-User-ID",
			"preferred_username": "X-Authenticated-User-Preferred-Name",
			"name": "X-Authenticated-User-FullName",
			"sub": "X-Authenticated-Subject",
			"email": "X-Authenticated-User-Email",
		},
		configs.EnvConfigs.GolangEnvironment)
	}

	// PE: Use these for configuring routes to leverage Permit.IO Authorization
	// defaultAuth := func(resource string, action string) func(*fiber.Ctx) error {
	// 	return middleware.NewDefaultAuthorizationMiddleware(
	// 		configs.EnvConfigs.AuthorizationUrl,
	// 		configs.EnvConfigs.AuthorizationKey,
	// 		resource,
	// 		action,
	// 	)
	// }
	// customAuth := func() func(*fiber.Ctx) error {
	// 	return middleware.NewCustomAuthorizationMiddleware(
	// 		configs.EnvConfigs.AuthorizationUrl,
	// 		configs.EnvConfigs.AuthorizationKey,
	// 	)
	// }


	route := app.Group(route_prefix + "v1/api")

	route.Get("/whoami", oktaAuthentication(), handlers.GetIdentity)
}
