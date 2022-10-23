package main

import (
	"flag"
	"github.com/stretchr/testify/assert"
	"slack-github.com/slack/murron/gen/proto/tracepb"
	"slack-github.com/slack/murron/pkg/traces"
	"testing"
)

type tagValueTest struct {
	argString        string  // cmd arg
	expectErr        bool    // should it return an error?
	expectedKey      string  // result tag key (if no error)
	expectedVBool    bool    // result tag bool value (if no error)
	expectedVFloat64 float64 // result tag float value (if no error)
	expectedVInt64   int64   // result tag int value (if no error)
	expectedVStr     string  // result tag string value (if no error)
}

func runTagValueTests(t *testing.T, setter flag.Value, expectedVType tracepb.ValueType, tests []*tagValueTest) {
	assert := assert.New(t)

	// tests that should always pass
	preTests := []*tagValueTest{
		{argString: "", expectErr: true},
		{argString: ":", expectErr: true},
		{argString: "key-only:", expectErr: true},
		{argString: ":true", expectErr: true},
		{argString: ":a_string", expectErr: true},
		{argString: ":12", expectErr: true},
		{argString: ":2.718", expectErr: true},
	}

	// setter.String() should always return empty string
	t.Run("String()", func(t *testing.T) {
		assert.Equal("", setter.String(), "default is always empty")
	})

	// loop through all the tests
	for _, test := range append(preTests, tests...) {
		t.Run(test.argString, func(t *testing.T) {
			// reset rawSpan and set a tag on it
			rawSpan = new(spanArgs)
			err := setter.Set(test.argString)

			// check if we should expect an error
			if test.expectErr {
				assert.Error(err)
				assert.Equal(0, len(rawSpan.tags), "len(rawSpan.tags)") // should not have a tag
			} else {
				// otherwise, there should be no error
				assert.NoError(err)
				// should have 1 tag in the rawSpan
				if assert.Equal(1, len(rawSpan.tags), "len(rawSpan.tags)") {
					tag := rawSpan.tags[0]
					assert.Equal(expectedVType, tag.VType, "tag.VType")
					assert.Equal(test.expectedKey, tag.Key, "tag.Key")
					assert.Equal(test.expectedVBool, tag.VBool, "tag.VBool")
					assert.Equal(test.expectedVFloat64, tag.VFloat64, "tag.VFloat64")
					assert.Equal(test.expectedVInt64, tag.VInt64, "tag.VInt64")
					assert.Equal(test.expectedVStr, tag.VStr, "tag.VStr")
				}
			}
		})
	}
}

func TestBoolTagValue(t *testing.T) {
	tests := []*tagValueTest{
		{argString: "test-key:not_a_bool", expectErr: true},
		{argString: "test-key:true", expectedKey: "test-key", expectedVBool: true},
		{argString: "test-key:false", expectedKey: "test-key", expectedVBool: false},
		{argString: "test-key:True", expectedKey: "test-key", expectedVBool: true},
		{argString: "test-key:False", expectedKey: "test-key", expectedVBool: false},
	}
	runTagValueTests(t, new(boolTagValue), tracepb.ValueType_BOOL, tests)
}

func TestFloatTagValue(t *testing.T) {
	tests := []*tagValueTest{
		{argString: "test-key:not_a_float", expectErr: true},
		{argString: "test-key:2.7.18", expectErr: true},
		{argString: "test-key:2.718", expectedKey: "test-key", expectedVFloat64: 2.718},
		{argString: "test-key:-2.718", expectedKey: "test-key", expectedVFloat64: -2.718},
	}
	runTagValueTests(t, new(floatTagValue), tracepb.ValueType_FLOAT64, tests)
}

func TestIntTagValue(t *testing.T) {
	tests := []*tagValueTest{
		{argString: "test-key:not_an_int", expectErr: true},
		{argString: "test-key:2.718", expectErr: true},
		{argString: "test-key:420", expectedKey: "test-key", expectedVInt64: 420},
		{argString: "test-key:-420", expectedKey: "test-key", expectedVInt64: -420},
	}
	runTagValueTests(t, new(intTagValue), tracepb.ValueType_INT64, tests)
}

func TestStringTagValue(t *testing.T) {
	tests := []*tagValueTest{
		{argString: "test-key:true", expectedKey: "test-key", expectedVStr: "true"},
		{argString: "test-key:420", expectedKey: "test-key", expectedVStr: "420"},
		{argString: "test-key:2.718", expectedKey: "test-key", expectedVStr: "2.718"},
		{argString: "test-key:a simple string", expectedKey: "test-key", expectedVStr: "a simple string"},
		{argString: "test-key:::colon::::separated::string:", expectedKey: "test-key", expectedVStr: "::colon::::separated::string:"},
	}
	runTagValueTests(t, new(stringTagValue), tracepb.ValueType_STRING, tests)
}

func TestSpanArgs_ToSpan(t *testing.T) {
	assert := assert.New(t)

	traces.NewSpanID = func() []byte { return []byte("new-id") }
	traces.NewTraceID = func() []byte { return []byte("new-trace-id") }

	for _, tt := range []struct {
		name     string
		spanArgs *spanArgs
		oneOff   bool
		want     *tracepb.Span
	}{
		{
			name: "all fields one-off=false",
			spanArgs: &spanArgs{
				id:             "mock-id",
				parentID:       "mock-parent-id",
				traceID:        "mock-trace-id",
				name:           "mock name",
				startMicros:    1576717447222827,
				durationMicros: 300,
				tags:           []*tracepb.KeyValue{traces.StringKV("test-key", "test-val")},
			},
			want: &tracepb.Span{
				Id:                   []byte("mock-id"),
				ParentId:             []byte("mock-parent-id"),
				TraceId:              []byte("mock-trace-id"),
				Name:                 "mock name",
				StartTimestampMicros: 1576717447222827,
				DurationMicros:       300,
				Tags:                 []*tracepb.KeyValue{traces.StringKV("test-key", "test-val")},
			},
		},
		{
			name:   "all fields one-off=true",
			oneOff: true,
			spanArgs: &spanArgs{
				id:             "mock-id",
				parentID:       "mock-parent-id",
				traceID:        "mock-trace-id",
				name:           "mock name",
				startMicros:    1576717447222827,
				durationMicros: 300,
				tags:           []*tracepb.KeyValue{traces.StringKV("test-key", "test-val")},
			},
			want: &tracepb.Span{
				Id:                   []byte("mock-id"),
				ParentId:             []byte("mock-parent-id"),
				TraceId:              []byte("mock-trace-id"),
				Name:                 "mock name",
				StartTimestampMicros: 1576717447222827,
				DurationMicros:       300,
				Tags:                 []*tracepb.KeyValue{traces.StringKV("test-key", "test-val")},
			},
		},
		{
			name:   "no id one-off=true",
			oneOff: true,
			spanArgs: &spanArgs{
				parentID:       "mock-parent-id",
				traceID:        "mock-trace-id",
				name:           "mock name",
				startMicros:    1576717447222827,
				durationMicros: 300,
				tags:           []*tracepb.KeyValue{traces.StringKV("test-key", "test-val")},
			},
			want: &tracepb.Span{
				Id:                   []byte("new-id"),
				ParentId:             []byte("mock-parent-id"),
				TraceId:              []byte("mock-trace-id"),
				Name:                 "mock name",
				StartTimestampMicros: 1576717447222827,
				DurationMicros:       300,
				Tags:                 []*tracepb.KeyValue{traces.StringKV("test-key", "test-val")},
			},
		},
		{
			name:   "no trace-id one-off=true",
			oneOff: true,
			spanArgs: &spanArgs{
				id:             "mock-id",
				parentID:       "mock-parent-id",
				name:           "mock name",
				startMicros:    1576717447222827,
				durationMicros: 300,
				tags:           []*tracepb.KeyValue{traces.StringKV("test-key", "test-val")},
			},
			want: &tracepb.Span{
				Id:                   []byte("mock-id"),
				ParentId:             []byte("mock-parent-id"),
				TraceId:              []byte("new-trace-id"),
				Name:                 "mock name",
				StartTimestampMicros: 1576717447222827,
				DurationMicros:       300,
				Tags:                 []*tracepb.KeyValue{traces.StringKV("test-key", "test-val")},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spanArgs.ToSpan()

			oneOff = tt.oneOff
			assert.Equal(string(tt.want.Id), string(got.Id), "span.Id")
			assert.Equal(string(tt.want.ParentId), string(got.ParentId), "span.ParentId")
			assert.Equal(string(tt.want.TraceId), string(got.TraceId), "span.TraceId")
			assert.Equal(tt.want.Name, got.Name, "span.Name")
			assert.Equal(tt.want.StartTimestampMicros, got.StartTimestampMicros, "span.TimestampMicros")
			assert.Equal(tt.want.DurationMicros, got.DurationMicros, "span.DurationMicros")
			assert.Equal(tt.want.Tags, got.Tags, "span.Tags")
		})
	}
}
