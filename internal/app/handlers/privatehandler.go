package handlers

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
)

const (
	header_userid = "X-Authenticated-User-ID"
)

func GetIdentity(ctx *fiber.Ctx) error {
	userid := ctx.Get(header_userid, "testuserid")

	return ctx.Status(fiber.StatusOK).SendString(fmt.Sprintf("Extracted user ID: %s", userid))
}