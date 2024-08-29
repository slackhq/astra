package com.slack.astra.server;

import brave.Tracing;
import brave.context.log4j2.ThreadContextScopeDecorator;
import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import com.google.common.util.concurrent.AbstractIdleService;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.brave.RequestContextCurrentTraceContext;
import com.linecorp.armeria.common.grpc.GrpcMeterIdPrefixFunction;
import com.linecorp.armeria.common.logging.LogLevel;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.brave.BraveService;
import com.linecorp.armeria.server.docs.DocService;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.encoding.EncodingService;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.logging.LoggingService;
import com.linecorp.armeria.server.management.ManagementService;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import com.slack.astra.proto.config.AstraConfigs;
import io.grpc.BindableService;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.reporter.Sender;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class ArmeriaService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ArmeriaService.class);

  private final String serviceName;
  private final Server server;

  private ArmeriaService(Server server, String serviceName) {
    this.server = server;
    this.serviceName = serviceName;
  }

  public static class Builder {
    private final String serviceName;
    private final ServerBuilder serverBuilder;
    private final List<SpanHandler> spanHandlers = new ArrayList<>();

    public Builder(int port, String serviceName, PrometheusMeterRegistry prometheusMeterRegistry) {
      this.serviceName = serviceName;
      this.serverBuilder = Server.builder().http(port);

      initializeCompression();
      initializeLogging();
      initializeManagementEndpoints(prometheusMeterRegistry);
    }

    private void initializeCompression() {
      serverBuilder.decorator(EncodingService.builder().newDecorator());
    }

    private void initializeLogging() {
      serverBuilder.decorator(
          LoggingService.builder()
              // Not logging any successful response, say prom scraping /metrics every 30 seconds at
              // INFO
              .successfulResponseLogLevel(LogLevel.DEBUG)
              .failureResponseLogLevel(LogLevel.ERROR)
              // Remove the content to prevent blowing up the logs
              .responseContentSanitizer((ctx, content) -> "truncated")
              // Remove all headers to be sure we aren't leaking any auth/cookie info
              .requestHeadersSanitizer((ctx, headers) -> DefaultHttpHeaders.EMPTY_HEADERS)
              .newDecorator());
    }

    private void initializeManagementEndpoints(PrometheusMeterRegistry prometheusMeterRegistry) {
      serverBuilder
          .service("/health", HealthCheckService.builder().build())
          .service("/metrics", (ctx, req) -> HttpResponse.of(prometheusMeterRegistry.scrape()))
          .serviceUnder("/internal/management", ManagementService.of())
          .serviceUnder("/docs", new DocService());
    }

    public Builder withRequestTimeout(Duration requestTimeout) {
      serverBuilder.requestTimeout(requestTimeout);
      return this;
    }

    public Builder withTracing(AstraConfigs.TracingConfig tracingConfig) {
      // span handlers is an ordered list, so we need to be careful with ordering
      if (tracingConfig.getCommonTagsCount() > 0) {
        spanHandlers.add(
            new SpanHandler() {
              @Override
              public boolean begin(TraceContext context, MutableSpan span, TraceContext parent) {
                tracingConfig.getCommonTagsMap().forEach(span::tag);
                return true;
              }
            });
      }

      if (!tracingConfig.getZipkinEndpoint().isBlank()) {
        LOG.info(String.format("Trace reporting enabled: %s", tracingConfig.getZipkinEndpoint()));
        Sender sender = URLConnectionSender.create(tracingConfig.getZipkinEndpoint());
        spanHandlers.add(AsyncZipkinSpanHandler.create(sender));
      }

      return this;
    }

    public Builder withAnnotatedService(Object service) {
      serverBuilder.annotatedService(service, DecodingService.newDecorator());
      return this;
    }

    public Builder withGrpcService(BindableService grpcService) {
      GrpcServiceBuilder searchBuilder =
          GrpcService.builder()
              .addService(grpcService)
              .enableUnframedRequests(true)
              // if not using the client timeout header - separate, lower timeouts
              // should be configured for indexer / cache nodes than that of the query server
              .useClientTimeoutHeader(true)
              .useBlockingTaskExecutor(true)
              .intercept(
                  new ServerInterceptor() {
                    // This method call adds the Interceptor to enable compressed server responses
                    // for all RPCs - see
                    // https://github.com/grpc/grpc-java/tree/d4fa0ecc07495097453b0a2848765f076b9e714c/examples/src/main/java/io/grpc/examples/experimental
                    @Override
                    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call,
                        Metadata headers,
                        ServerCallHandler<ReqT, RespT> next) {
                      call.setCompression("gzip");
                      return next.startCall(call, headers);
                    }
                  });
      serverBuilder.decorator(
          MetricCollectingService.newDecorator(GrpcMeterIdPrefixFunction.of("grpc.service")));
      serverBuilder.service(searchBuilder.build());
      return this;
    }

    public ArmeriaService build() {
      Tracing.Builder tracingBuilder =
          Tracing.newBuilder()
              .localServiceName(serviceName)
              .currentTraceContext(
                  RequestContextCurrentTraceContext.builder()
                      .addScopeDecorator(ThreadContextScopeDecorator.get())
                      .build());
      spanHandlers.forEach(tracingBuilder::addSpanHandler);
      serverBuilder.decorator(BraveService.newDecorator(tracingBuilder.build()));

      return new ArmeriaService(serverBuilder.build(), serviceName);
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {} on ports {}", serviceName, server.config().ports());
    CompletableFuture<Void> serverFuture = server.start();
    serverFuture.get(15, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down");

    // On server close there is an option for a graceful shutdown, which is disabled by default.
    // When it is
    // disabled it immediately starts rejecting requests and begins the shutdown process, which
    // includes
    // running any remaining AsyncClosableSupport closeAsync actions. We want to allow this to take
    // up to the
    // maximum permissible shutdown time to successfully close.
    server.close();
  }

  @Override
  protected String serviceName() {
    if (this.serviceName != null) {
      return this.serviceName;
    }
    return super.serviceName();
  }

  @Override
  public String toString() {
    return "ArmeriaService{" + "serviceName='" + serviceName() + '\'' + '}';
  }
}
