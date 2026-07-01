package routes

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"refresher/trade-refresher/configs"

	swagger "github.com/arsmn/fiber-swagger/v2"
	"github.com/gofiber/fiber/v2"
)


func echoHandler(ctx *fiber.Ctx) error {
	message := ctx.Query("message")
	return ctx.JSON(fiber.Map{
		"message": "Received echo request: " + message,
	})
}

func envHandler(ctx *fiber.Ctx) error {
	envJson, err := json.Marshal(configs.EnvConfigs)
	if err != nil {
		return ctx.SendStatus(fiber.StatusBadRequest)
	}
	ctx.Set("Content-Type", "application/json")
	return ctx.Send(envJson)
}

func UtilityRoutes(app *fiber.App) {
	app.Get(route_prefix + "v1/api/echo", echoHandler)
	app.Get(route_prefix + "v1/api/env", envHandler)
	app.Get(route_prefix + "v1/api/swagger", swagger.HandlerDefault)

	// RB: isolate the health check on a separate goroutine to ensure it is never blocked

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("status: healthy"))
		})

		srv := &http.Server{
			Addr:              ":8080",
			ReadHeaderTimeout: time.Duration(10 * int(time.Second)),
			ReadTimeout:       time.Duration(10 * int(time.Second)),
			WriteTimeout:      time.Duration(10 * int(time.Second)),
			IdleTimeout:       time.Duration(1 * int(time.Hour)),
			Handler:           mux,
		}

		if err := srv.ListenAndServe(); err != nil {
			panic(fmt.Sprintf("utilityroutes.go: health handler has thrown an error: %v", err))
		}
	}()

	app.Use(
		func(ctx *fiber.Ctx) error {
			return ctx.SendStatus(fiber.StatusNotFound)
		},
	)
}
