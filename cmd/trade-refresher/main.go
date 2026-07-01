package main

import (
	"context"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel"
	stdout "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"refresher/trade-refresher/configs"
	"refresher/trade-refresher/internal/app/handlers"
	"refresher/trade-refresher/internal/middleware"
	"refresher/trade-refresher/internal/routes"
	"refresher/trade-refresher/internal/utils/log"
	"refresher/trade-refresher/internal/utils/net"
)

func main() {
	configs.Load()

	tp := newTracerProvider()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Logger.Error(fmt.Sprintf("unable to shutdown the trace provider: %v", err))
		}
	}()

	prepare()

	app := fiber.New()
	middleware.FiberMiddleware(app)

	routes.PublicRoutes(app)
	routes.PrivateRoutes(app, privateRouteHandlers())
	routes.UtilityRoutes(app)

	net.StartServer(app)
}

func newTracerProvider() *sdktrace.TracerProvider {
	exporter, err := stdout.New(stdout.WithPrettyPrint())
	if err != nil {
		log.Logger.Fatal(fmt.Sprintf("unable to create a new OTEL tracer provider: %v", err))
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("trade-refresher"),
			)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp
}

func prepare() {

}

// privateRouteHandlers configures specific handlers for the non-prod/prod environments.  For some external API calls, there is only a single environment.  For non-production
// it is recommended to simulate/mock the return responses to avoid resources being consumed/created on external platforms (i.e. GitLab, Permit.IO, etc.)
func privateRouteHandlers() routes.Handlers {
	if configs.EnvConfigs.GolangEnvironment.IsLocal() {
		return routes.Handlers{
			GetIdentity: handlers.GetIdentity,
		}
	}

	if configs.EnvConfigs.GolangEnvironment.IsProduction() {
		return routes.Handlers{
			GetIdentity: handlers.GetIdentity,
		}
	}

	return routes.Handlers{
		GetIdentity: handlers.GetIdentity,
	}
}