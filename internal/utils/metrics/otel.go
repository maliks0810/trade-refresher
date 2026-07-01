package metrics

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Span will create a new instance of the OTEL Span interface for consistent metric tracking/publishing
//
// Parameters:
//
// - ctx : context.Context The context to execute the tracer against (usually Background)
//
// - functionName : string The specific function that is being traced
//
// - spanName : string The unique name of the span
//
// - attributes : attribute.KeyVault Variable collection of attributes to add to the trace span
func Span(ctx context.Context, functionName string, spanName string, attributes ...attribute.KeyValue) trace.Span {
	tracer := otel.Tracer(functionName)

	_, s := tracer.Start(ctx, spanName, spanOptions(attributes...))

	return s
}

func spanOptions(keyvalues ...attribute.KeyValue) trace.SpanStartOption {
	return trace.WithAttributes(keyvalues...)
}