// A tool to send spans to murron-agent.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	math_rand "math/rand"
	"os"
	proto "spangen/spangen/generated"
	"strconv"
	"strings"
	"time"
)

// Trace helpers
const PipelineTagKey = "__dataset"

// create a seeded rand
var rand = math_rand.New(math_rand.NewSource(time.Now().UnixNano()))

// Generate trace and span id valid per open tracing specs
// https://github.com/opentracing/specification/blob/master/rfc/trace_identifiers.md
var NewTraceID = func() []byte {
	hi := rand.Uint64()
	lo := rand.Uint64()
	if hi == 0 {
		return []byte(fmt.Sprintf("%x", lo))
	}
	return []byte(fmt.Sprintf("%x%016x", hi, lo))
}

var NewSpanID = func() []byte {
	return []byte(fmt.Sprintf("%x", rand.Uint64()))
}

func reporterHelp() string {
	str := "allowed reporters:"
	for k := range reporters {
		str += " " + k
	}
	return str
}

func StringKV(key, val string) *proto.KeyValue {
	return makeKV(key, val, proto.ValueType_STRING)
}
func BinaryKV(key string, val []byte) *proto.KeyValue {
	return makeKV(key, val, proto.ValueType_BINARY)
}
func BoolKV(key string, val bool) *proto.KeyValue {
	return makeKV(key, val, proto.ValueType_BOOL)
}
func Float64KV(key string, val float64) *proto.KeyValue {
	return makeKV(key, val, proto.ValueType_FLOAT64)
}
func Int64KV(key string, val int64) *proto.KeyValue {
	return makeKV(key, val, proto.ValueType_INT64)
}
func makeKV(key string, val interface{}, vtype proto.ValueType) *proto.KeyValue {
	kv := &proto.KeyValue{Key: key, VType: vtype}
	switch vtype {
	case proto.ValueType_STRING:
		kv.VStr = val.(string)
	case proto.ValueType_BINARY:
		kv.VBinary = val.([]byte)
	case proto.ValueType_BOOL:
		kv.VBool = val.(bool)
	case proto.ValueType_FLOAT64:
		kv.VFloat64 = val.(float64)
	case proto.ValueType_INT64:
		kv.VInt64 = val.(int64)
	}
	return kv
}

// Span gen code.
var (
	// set from cmd flags
	rawSpan     *spanArgs
	verFlag     bool
	verbose     bool
	reporter    string
	oneOff      bool
	spanDataset string
)

// list of reporter end points
var reporters = map[string]Reporter{
	"noop":    new(noopReporter),
	"console": new(consoleReporter),
	// TODO: Add a http reporter.
}

func init() {
	rawSpan = new(spanArgs)

	// span flags
	flag.StringVar(&rawSpan.id, "id", "", "span id (required unless using -one-off)")
	flag.StringVar(&rawSpan.parentID, "parent-id", "", "parent id (leave empty for root span)")
	flag.StringVar(&rawSpan.traceID, "trace-id", "", "trace id (required unless using -one-off)")
	flag.StringVar(&rawSpan.name, "name", "", "name for the event")
	flag.Int64Var(&rawSpan.durationMicros, "duration-micros", 0, "duration of event in microseconds (required)")
	flag.Int64Var(&rawSpan.startMicros, "start-micros", 0, "start of event in microseconds since epoch (required)")
	flag.BoolVar(&oneOff, "one-off", false, "use a random span id and trace id")
	// TODO: Set a dataset tag.
	flag.StringVar(&spanDataset, "dataset", "", "set a dataset tag")

	// tags for span flags
	flag.Var(new(stringTagValue), "string-tag", "tag formatted as key:value, where value is a string (repeatable)")
	flag.Var(new(boolTagValue), "bool-tag", "tag formatted as key:value, where value is true or false (repeatable)")
	flag.Var(new(intTagValue), "int-tag", "tag formatted as key:value, where value is an integer (repeatable)")
	flag.Var(new(floatTagValue), "float-tag", "tag formatted as key:value, where value is a float (repeatable)")

	// how we emit the spans
	flag.StringVar(&reporter, "reporter", "console", "tell the generator how to report spans; "+reporterHelp())

	// common flags
	flag.BoolVar(&verFlag, "version", false, "print version and quit")
	flag.BoolVar(&verbose, "verbose", false, "print results as json object")
}

// span args provided by flags
type spanArgs struct {
	id, parentID, traceID, name string
	startMicros, durationMicros int64

	tags []*proto.KeyValue
}

// appends to tags
func (x *spanArgs) appendTag(tag *proto.KeyValue) { x.tags = append(x.tags, tag) }

// convert spanArgs into a span pb
func (x *spanArgs) ToSpan() *proto.Span {
	if oneOff && x.id == "" {
		x.id = string(NewSpanID())
	}
	if oneOff && x.traceID == "" {
		x.traceID = string(NewTraceID())
	}
	return &proto.Span{
		Id:        []byte(x.id),
		ParentId:  []byte(x.parentID),
		TraceId:   []byte(x.traceID),
		Name:      x.name,
		Timestamp: uint64(x.startMicros),
		Duration:  uint64(x.durationMicros),
		Tags:      x.tags,
	}
}

// implements the String() method for tag values
type tagValue struct{}

// no defaults for any of the tags args
func (tagValue) String() string { return "" }

// implements the Set() method for string tags
type stringTagValue struct{ tagValue }

func (stringTagValue) Set(arg string) error {
	// parse args into kv pair
	key, val, err := parseTagArg(arg)
	if err != nil {
		return err
	}
	// format as string kv and append to all tags
	rawSpan.appendTag(StringKV(key, val))
	return nil
}

// implements the Set() method for bool tags
type boolTagValue struct{ tagValue }

func (boolTagValue) Set(arg string) error {
	// parse args into kv pair
	key, rawVal, err := parseTagArg(arg)
	if err != nil {
		return err
	}
	// parse rawVal as bool
	val, err := strconv.ParseBool(rawVal)
	if err != nil {
		return err
	}
	// format as bool kv and append to all tags
	rawSpan.appendTag(BoolKV(key, val))
	return nil
}

type intTagValue struct{ tagValue }

func (intTagValue) Set(arg string) error {
	// parse args into kv pair
	key, rawVal, err := parseTagArg(arg)
	if err != nil {
		return err
	}
	// parse rawVal as base10, 64bit integer
	val, err := strconv.ParseInt(rawVal, 10, 64)
	if err != nil {
		return err
	}
	// format as int kv and append to all tags
	rawSpan.appendTag(Int64KV(key, val))
	return nil
}

type floatTagValue struct{ tagValue }

func (floatTagValue) Set(arg string) error {
	// parse args into kv pair
	key, rawVal, err := parseTagArg(arg)
	if err != nil {
		return err
	}
	// parse rawVal as 64bit float
	val, err := strconv.ParseFloat(rawVal, 64)
	if err != nil {
		return err
	}
	// format as float kv and append to all tags
	rawSpan.appendTag(Float64KV(key, val))
	return nil
}

// splits a tag arg into key and string value
func parseTagArg(argString string) (string, string, error) {
	args := strings.SplitN(argString, ":", 2)
	if len(args) != 2 {
		return "", "", fmt.Errorf("expected format <key>:<value>")
	}
	key, val := args[0], args[1]
	if key == "" {
		return "", "", fmt.Errorf("key empty")
	}
	if val == "" {
		return "", "", fmt.Errorf("value empty")
	}
	return key, val, nil
}

func main() {
	// parse flags
	flag.Parse()

	spanReporter := reporters[reporter]
	if spanReporter == nil {
		_, _ = fmt.Fprintf(os.Stderr, "unknown reporter %s\n", reporter)
		_, _ = fmt.Fprintln(os.Stderr, reporterHelp())
		os.Exit(1)
	}

	// read span from command args
	span := rawSpan.ToSpan()

	// set the span dataset
	if spanDataset != "" {
		span.Tags = append(span.Tags, StringKV(PipelineTagKey, spanDataset))
	}

	// report the span
	err := spanReporter.Report(span)
	exitOnError(span, 1, "spanReporter.Report() failed", err)

	// we're done!
	exitSuccess(span)
}

// prints error to stderr
// prints results json to stdout if verbose
// exits the program with given code
func exitOnError(span *proto.Span, code int, pre string, err error) {
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", pre, err)
		printResults(span, err)
		os.Exit(code)
	}
}

// prints results json to stdout if verbose
// exists the program with code 0
func exitSuccess(span *proto.Span) {
	printResults(span, nil)
	os.Exit(0)
}

// prints results json to stdout if verbose
func printResults(span *proto.Span, err error) {
	// print the span back to user as json
	if verbose {
		msg := map[string]interface{}{"span": span, "ok": err == nil}
		if err != nil {
			msg["error"] = err.Error()
		}
		msgJSON, _ := json.Marshal(msg)
		fmt.Println(string(msgJSON))
	}
}
