package auth

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/keyauth"

	"refresher/trade-refresher/internal/utils/types"
)

func NewKeyAuthConfigs(scheme string, handler AuthHandler) keyauth.Config {
	return keyauth.Config{
		ErrorHandler: handler.Error,
		Validator:    handler.Validator,
		KeyLookup:    "header:" + fiber.HeaderAuthorization,
		AuthScheme:   scheme,
		ContextKey:   "token",
	}
}

type FiberKeyAuth struct {
	Handler     AuthHandler
	Environment types.Environment
	Cache       types.TTLCache
	CacheKy     string
}

func (a FiberKeyAuth) Error(ctx *fiber.Ctx, err error) error {
	if !a.Environment.IsLocal() {
		if err == keyauth.ErrMissingOrMalformedAPIKey {
			return ctx.Status(fiber.StatusUnauthorized).SendString("API Authorization Token not supplied - Unable to Authenticate")
		}
		return ctx.Status(fiber.StatusUnauthorized).SendString("API Authorization Token was malformed, invalid, or expired - Unable to Authenticate")
	}
	return ctx.Next()
}

func (a FiberKeyAuth) Validator(ctx *fiber.Ctx, key string) (bool, error) {
	if !a.Environment.IsLocal() {
		token, err := a.Cache.Get(a.CacheKy)
		if err != nil {
			return false, fmt.Errorf("unable to retrieve cached API token value: %w", err)
		}
		return (token == key), nil
	}
	return true, nil
}
