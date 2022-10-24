package main

import (
	"vendor/com.slack/kaldb/gen/proto/tracepb/tracepb"
)

// Interface for reporting a span
type Reporter interface {
	Report(span *tracepb.Span) error
}

// does nothing
type noopReporter struct{}

func (x *noopReporter) Report(span *tracepb.Span) error { return nil }
