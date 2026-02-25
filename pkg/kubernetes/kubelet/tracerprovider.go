package kubelet

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var tracerLog = newLogger("tracer")

// TracerProvider wraps the no-op tracer provider with warn logging.
type TracerProvider struct {
	noop.TracerProvider
}

func (TracerProvider) Tracer(name string, options ...trace.TracerOption) trace.Tracer {
	tracerLog.Debug().Str("name", name).Msg("Tracer not implemented")
	return Tracer{}
}

// Tracer wraps the no-op tracer with warn logging.
type Tracer struct {
	noop.Tracer
}

func (Tracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracerLog.Debug().Str("span", spanName).Msg("Start not implemented")
	return ctx, Span{}
}

// Span wraps the no-op span with warn logging.
type Span struct {
	noop.Span
}

func (Span) End(options ...trace.SpanEndOption) {
	tracerLog.Debug().Msg("End not implemented")
}

func (Span) SetName(name string) {
	tracerLog.Debug().Str("name", name).Msg("SetName not implemented")
}

func (Span) SetStatus(code codes.Code, description string) {
	tracerLog.Debug().Str("code", code.String()).Str("description", description).Msg("SetStatus not implemented")
}

func (Span) SetAttributes(kv ...attribute.KeyValue) {
	tracerLog.Debug().Msg("SetAttributes not implemented")
}

func (Span) RecordError(err error, options ...trace.EventOption) {
	tracerLog.Debug().Err(err).Msg("RecordError not implemented")
}

func (Span) AddEvent(name string, options ...trace.EventOption) {
	tracerLog.Debug().Str("name", name).Msg("AddEvent not implemented")
}

func (Span) AddLink(link trace.Link) {
	tracerLog.Debug().Msg("AddLink not implemented")
}

func (Span) IsRecording() bool {
	tracerLog.Debug().Msg("IsRecording not implemented")
	return false
}
