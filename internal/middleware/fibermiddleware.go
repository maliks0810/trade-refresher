package middleware

import (
	"time"

	"github.com/gofiber/contrib/otelfiber"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

func FiberMiddleware(application *fiber.App) {
	application.Use(
		otelfiber.Middleware(),
		compress.New(compressionConfig()),
		cors.New(corsConfig()),
		limiter.New(limiterConfig()),
		logger.New(loggerConfig()),
		recover.New(recoverConfig()),
	)
}

func compressionConfig() compress.Config {
	return compress.Config{
		Level: compress.LevelBestSpeed,
	}

}

func corsConfig() cors.Config {
	return cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-Type, Accept, Authorization",
		AllowMethods: "OPTIONS,GET,POST,HEAD,PUT,DELETE,PATCH",
	}
}

func limiterConfig() limiter.Config {
	return limiter.Config{
		Max: 				120,
		Expiration: 		1 * time.Minute,
		LimiterMiddleware: 	limiter.FixedWindow{},
		LimitReached: func(ctx *fiber.Ctx) error {
			ctx.Context().Logger().Printf("Rate Limit has been reached!")
			return ctx.SendStatus(fiber.StatusTooManyRequests)
		},
	}
}

func loggerConfig() logger.Config {
	return logger.Config{
		Format: "[${time}] ${status} - ${latency} ${method} ${path}\n",
		TimeFormat: "15:04:05", // https://programming.guide/go/format-parse-string-time-date-example.html
		TimeZone: "Local",
	}
}

func recoverConfig() recover.Config {
	return recover.Config{
		EnableStackTrace: true,
	}
}
