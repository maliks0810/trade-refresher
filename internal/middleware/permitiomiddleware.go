package middleware

import (
	"github.com/gofiber/fiber/v2"

	"refresher/trade-refresher/internal/utils/auth"
)

// NewDefaultAuthorizationMiddleware will create custom Permit.IO middleware designed to validate authorization
// for predefined resource and actions.  If not using specific resource instances, or customized authorization checks
// this should be the default authorization middleware to leverage
//
// Parameters:
//
// host - string : The host URL of the Permit.IO Policy Decision Point
//
// token - string : The Permit.IO API Key of the authorization project and environment
//
// resource - string : The name of the resource defined in the Permit.IO policy map
//
// action - string : The action to be taken against the resource defined in the Permit.IO policy map
func NewDefaultAuthorizationMiddleware(host string, token string, resource string, action string) func(*fiber.Ctx) error {
	pc := auth.PermitIOConfig{
		Url: host,
		Token: token,
		Resource: resource,
		Action: action,
	}

	return auth.NewPermitIOMiddleware(pc)
}


// NewCustomAuthorizationMiddleware will configure middleware that is designed to enable communication with the
// Permit.IO Policy Decision Point, but does not automatically check a resource/action.  Use this middleware
// for resource instances, or custom authorization check(s).  For advanced use cases only.
//
// Parameters:
//
// host - string : The host URL of the Permit.IO Policy Decision Point
//
// token - string : The Permit.IO API Key of the authorization project and environment
func NewCustomAuthorizationMiddleware(host string, token string) func(*fiber.Ctx) error {
	pc := auth.PermitIOConfig{
		Url: host,
		Token: token,
		Resource: "",
		Action: "",
	}

	return auth.NewPermitIOMiddleware(pc)
}
