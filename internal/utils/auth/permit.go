package auth

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/permitio/permit-golang/pkg/config"
	"github.com/permitio/permit-golang/pkg/enforcement"
	"github.com/permitio/permit-golang/pkg/permit"
)

const (
	pdp_sidecar_url = "http://localhost:7000"
)

type PermitIOConfig struct {
	// Url is the specific URL of the Permit.IO Policy Decision Point
	Url 				string
	// Token is the specific API Key for the Permit.IO project/environment
	Token 				string
	// Resource is the specific Permit.IO resource to authorize
	Resource 			string
	// Action is the specific Permit.IO action to authorize
	Action 				string
	// Validator will check the authorization status of the resource/action with Permit.IO
	Validator 			func(*fiber.Ctx, string, string) (bool, error)
	// SuccessHandler will be executed if the authorization check succeeds
	SuccessHandler 		fiber.Handler
	// UnauthorizedHandler will be executed if the authorization check fails
	UnauthorizedHandler	fiber.Handler
	// ErrorHandler will be executed if the authorization check fails to communicate with Permit.IO PDP
	ErrorHandler 		fiber.ErrorHandler
	// Next will execute the next middleware call within the middleware dependency chain
	Next 			func(*fiber.Ctx) bool
}

// PermitIOConfigDefault is a representation of the default values and implementation of the PermitIOConfig.  Provides
// simplicity, but still exposes customization based on the application owner desires.
var PermitIOConfigDefault = PermitIOConfig{
	Url: pdp_sidecar_url,
	Token: "",
	// Validator will check if the authenticated user has proper authorization against the provided resource and action
	//
	// Parameters:
	// ctx - *fiber.Ctx : The request context
	//
	// resource - string : The specific resource defined in the Permit.IO policy map
	//
	// action - string : The specific action on the associated resource defined in the Permit.IO policy map
	Validator: func(ctx *fiber.Ctx, resource string, action string) (bool, error) {
		return Check(ctx, resource, action)
	},
	// SuccessHandler will default to calling the internal Next() method to continue the middleware dependency chain
	//
	// Parameters:
	//
	// ctx - *fiber.Ctx : The request context
	SuccessHandler: func(ctx *fiber.Ctx) error {
		return ctx.Next()
	},
	// UnauthorizedHandler will return an HTTP 403 Forbidden if the authorization check returns an unauthorized indicator.
	//
	// Parameters:
	//
	// ctx - *fiber.Ctx : The request context
	UnauthorizedHandler: func(ctx *fiber.Ctx) error {
		return ctx.Status(fiber.StatusForbidden).SendString("Forbidden: User is not authorized for the requested resource")
	},
	// ErrorHandler will return an HTTP 403 Forbidden if the middleware was unable to communicate with the Permit.IO Policy Decision Point
	//
	// Parameters:
	//
	// ctx - *fiber.Ctx : The request context
	ErrorHandler: func(ctx *fiber.Ctx, err error) error {
		return ctx.Status(fiber.StatusForbidden).SendString(fmt.Sprintf("Forbidden: Unable to communicate with Permit.IO: %v", err))
	},
}

// NewPermitIOMiddleware will instantiate a specific fiber.Handler method with the provided configuration.  This middleware handler can
// be attached to routes (typically private) to ensure Permit.IO Policy Decision Point connectivity is established as well as automating
// authorization checks on known resources and actions.
//
// Parameters:
//
// config - PermitIOConfig (optional) : The custom configuration for Permit.IO Authorization middleware
func NewPermitIOMiddleware(config ...PermitIOConfig) fiber.Handler {
	cfg := permitConfigDefault(config...)

	return func(ctx *fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		if cfg.Url != "" {
			ctx.Request().Header.Add("X-PDP-Host", cfg.Url)
		}
		if cfg.Token != "" {
			ctx.Request().Header.Add("X-PDP-Token", cfg.Token)
		}
		
		// If resource/action not set, assume the check will be manually executed
		// in the handler
		if cfg.Resource == "" && cfg.Action == "" {
			return ctx.Next()
		}

		ok, err := cfg.Validator(ctx, cfg.Resource, cfg.Action)
		if err != nil {
			return cfg.ErrorHandler(ctx, err)
		}
		if !ok {
			return cfg.UnauthorizedHandler(ctx)
		}
	
		return cfg.SuccessHandler(ctx)
	}
}

func permitConfigDefault(config ...PermitIOConfig) PermitIOConfig {
	if len(config) < 1 {
		return PermitIOConfigDefault	
	}

	cfg := config[0]

	if cfg.Url == "" {
		cfg.Url = pdp_sidecar_url
	}
	if cfg.SuccessHandler == nil {
		cfg.SuccessHandler = PermitIOConfigDefault.SuccessHandler
	}
	if cfg.UnauthorizedHandler == nil {
		cfg.UnauthorizedHandler = PermitIOConfigDefault.UnauthorizedHandler
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = PermitIOConfigDefault.ErrorHandler
	}
	if cfg.Validator == nil {
		cfg.Validator = PermitIOConfigDefault.Validator
	}
	return cfg
}

func Check(ctx *fiber.Ctx, resource string, action string) (bool, error) {
	userid := strings.ToLower(ctx.Get(AuthenticatedUserHeaderKey, ""))
	if userid == "" {
		return false, errors.New("user has not been authenticated")
	}
	url := ctx.Get(PermitIOHostHeaderKey, "")
	if url == "" {
		return false, errors.New("invalid PDP host provided")
	}
	token := ctx.Get(PermitIOTokenHeaderKey, "")
	if token == "" {
		return false, errors.New("invalid PDP token provided")
	}

	u := enforcement.UserBuilder(userid).Build()
	a := enforcement.Action(action)
	var r enforcement.Resource
	if strings.Contains(resource, ":") {
		instances := strings.Split(resource, ":")
		r = enforcement.ResourceBuilder(instances[0]).WithKey(instances[1]).Build()
	} else {
		r = enforcement.ResourceBuilder(resource).Build()
	}

	config := config.NewConfigBuilder(token).WithPdpUrl(url).Build()
	client := permit.New(config)

	return client.Check(u, a, r) 
}