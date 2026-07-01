package auth

import (
	"github.com/gofiber/fiber/v2"
)

type AuthHandler interface {
	Error(*fiber.Ctx, error) error
	Validator(*fiber.Ctx, string) (bool, error)
}
