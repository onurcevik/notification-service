package telemetry

import (
	"context"
	"io"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// InitTracer initializes the global tracer with a stdout exporter and returns a shutdown function.
// If w is nil, os.Stderr is used.
func InitTracer(w io.Writer) (func(context.Context) error, error) {
	if w == nil {
		w = os.Stderr
	}
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(w))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			"",
			attribute.String("service.name", "notification-service"),
		)),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
