package com.slack.kaldb.writer;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.service.murron.trace.Trace;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.proto3.ListOfSpans;
import zipkin2.proto3.Span;

public final class OpenZipkinSpanFormatter {
  public static final Logger LOG = LoggerFactory.getLogger(OpenZipkinSpanFormatter.class);

  public static List<Trace.Span> toSpans(final ListOfSpans openZipkinSpansList) {
    final List<Span> openZipkinSpans = openZipkinSpansList.getSpansList();
    if (openZipkinSpans.isEmpty()) {
      return Collections.emptyList();
    }

    return openZipkinSpans
        .stream()
        .map(
            span -> {
              final Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();
              spanBuilder.setName(span.getName());
              spanBuilder.setId(span.getId());
              spanBuilder.setTraceId(span.getTraceId());
              spanBuilder.setStartTimestampMicros(span.getTimestamp());
              spanBuilder.setDurationMicros(span.getDuration());
              spanBuilder.setParentId(span.getParentId());

              final List<Trace.KeyValue> tags = new ArrayList<>(span.getTagsCount());
              if (span.getTagsCount() > 0) {
                tags.addAll(
                    span.getTagsMap()
                        .entrySet()
                        .stream()
                        .map(
                            entry -> {
                              final Trace.KeyValue.Builder tagBuilder = Trace.KeyValue.newBuilder();
                              tagBuilder.setKey(entry.getKey());
                              tagBuilder.setVType(Trace.ValueType.STRING);
                              tagBuilder.setVStr(entry.getValue());
                              return tagBuilder.build();
                            })
                        .collect(Collectors.toList()));
              }

              tags.add(
                  Trace.KeyValue.newBuilder()
                      .setKey(LogMessage.ReservedField.SERVICE_NAME.fieldName)
                      .setVType(Trace.ValueType.STRING)
                      .setVStr(span.getLocalEndpoint().getServiceName())
                      .build());

              if (span.getKind() != Span.Kind.SPAN_KIND_UNSPECIFIED) {
                tags.add(
                    Trace.KeyValue.newBuilder()
                        .setKey("SpanKind")
                        .setVType(Trace.ValueType.STRING)
                        .setVStr(span.getKind().name())
                        .build());
              }
              spanBuilder.addAllTags(tags);

              return spanBuilder.build();
            })
        .collect(Collectors.toList());
  }
}
