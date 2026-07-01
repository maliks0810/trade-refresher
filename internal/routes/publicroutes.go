package routes

import (
	"github.com/gofiber/fiber/v2"

	"refresher/trade-refresher/internal/app/handlers"
)

func PublicRoutes(app *fiber.App) {
	route := app.Group(route_prefix + "v1/api")

	route.Get("/info", handlers.GetInformation)
}
