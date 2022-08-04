package com.slack.kaldb.zipkinApi;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.*;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
@SuppressWarnings(
        "OptionalUsedAsFieldOrParameterType")
public class ZipkinService {
    private final KaldbQueryServiceBase searcher;

    public ZipkinService(KaldbQueryServiceBase searcher) {
        this.searcher = searcher;
    }

    @Get
    @Path("/api/v2/services")
    public HttpResponse getServices() throws IOException {
        String output = "[]";
        return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
    }

    @Get("/api/v2/spans")
    public HttpResponse getSpans(@Param("serviceName") Optional<String> serviceName) throws IOException {
        String output = "[]";
        return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
    }

    @Get("/api/v2/traces")
    public HttpResponse getTraces(
            @Param("serviceName") Optional<String> serviceName,
            @Param("spanName") Optional<String> spanName,
            @Param("annotationQuery") Optional<String> annotationQuery,
            @Param("minDuration") Optional<Integer> minDuration,
            @Param("maxDuration") Optional<Integer> maxDuration,
            @Param("endTs") Long endTs,
            @Param("lookback") Long lookback,
            @Param("limit") @Default("10") Integer limit)
            throws IOException {
        String output = "[]";
        return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
    }

    @Get("/api/v2/trace/{traceId}")
    public HttpResponse getTraceByTraceId(@Param("traceId") String traceId) throws IOException {
        String output = "[]";
        return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
    }
}