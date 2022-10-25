package main

import (
	"encoding/json"
	"fmt"
	proto "spangen/spangen/generated"
)

// Interface for reporting a span
type Reporter interface {
	Report(span *proto.Span) error
}

// does nothing
type noopReporter struct{}

func (x *noopReporter) Report(span *proto.Span) error { return nil }

// does nothing
type consoleReporter struct{}

func (x *consoleReporter) Report(span *proto.Span) error {
	jsonBytes, _ := json.MarshalIndent(span, "", "    ")
	fmt.Println(string(jsonBytes))
	return nil
}
