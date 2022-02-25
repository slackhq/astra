package com.slack.kaldb.server;

import static com.slack.kaldb.config.KaldbConfig.ARMERIA_TIMEOUT_DURATION;

import brave.Tracing;
import brave.context.log4j2.ThreadContextScopeDecorator;
import brave.handler.SpanHandler;
import com.google.common.util.concurrent.AbstractIdleService;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.brave.RequestContextCurrentTraceContext;
import com.linecorp.armeria.common.grpc.GrpcMeterIdPrefixFunction;
import com.linecorp.armeria.common.logging.LogLevel;
import com.linecorp.armeria.common.util.EventLoopGroups;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.brave.BraveService;
import com.linecorp.armeria.server.docs.DocService;
import com.linecorp.armeria.server.encoding.EncodingService;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.logging.LoggingService;
import com.linecorp.armeria.server.logging.LoggingServiceBuilder;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.elasticsearchApi.ElasticsearchApiService;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.reporter.Sender;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class ArmeriaService extends AbstractIdleService {
  private final Logger LOG = LoggerFactory.getLogger(ArmeriaService.class);

  private final PrometheusMeterRegistry prometheusMeterRegistry;
  private final String serviceName;
  private final Server server;

  private ArmeriaService(
      Server server, String serviceName, PrometheusMeterRegistry prometheusMeterRegistry) {
    this.server = server;
    this.serviceName = serviceName;
    this.prometheusMeterRegistry = prometheusMeterRegistry;
  }

  public static class Builder {
    private final Logger LOG = LoggerFactory.getLogger(ArmeriaService.Builder.class);
    private static final int WORKER_EVENT_LOOP_THREADS = 16;

    private final String serviceName;
    private final PrometheusMeterRegistry prometheusMeterRegistry;
    private final ServerBuilder serverBuilder;

    public Builder(int port, String serviceName, PrometheusMeterRegistry prometheusMeterRegistry) {
      this.serviceName = serviceName;
      this.prometheusMeterRegistry = prometheusMeterRegistry;

      ServerBuilder serverBuilder = Server.builder().http(port);
      initializeEventLoop(serverBuilder);
      setTimeout(serverBuilder);
      addCompression(serverBuilder);
      addManagementEndpoints(serverBuilder);
      addTracing(serverBuilder);

      this.serverBuilder = serverBuilder;
    }

    public Builder withElasticsearchApi(KaldbServiceGrpc.KaldbServiceImplBase searcher) {
      this.serverBuilder.annotatedService(new ElasticsearchApiService(searcher));
      return this;
    }

    public Builder withGrpcSearchApi(KaldbServiceGrpc.KaldbServiceImplBase searcher) {
      GrpcServiceBuilder searchBuilder =
          GrpcService.builder()
              .addService(searcher)
              .enableUnframedRequests(true)
              .useBlockingTaskExecutor(true);
      this.serverBuilder.decorator(
          MetricCollectingService.newDecorator(GrpcMeterIdPrefixFunction.of("grpc.service")));
      this.serverBuilder.service(searchBuilder.build());
      return this;
    }

    public ArmeriaService build() {
      return new ArmeriaService(serverBuilder.build(), serviceName, prometheusMeterRegistry);
    }

    // todo - cluster manager api
    // public Builder withClusterManagementApi() { }

    private void initializeEventLoop(ServerBuilder serverBuilder) {
      EventLoopGroup eventLoopGroup = EventLoopGroups.newEventLoopGroup(WORKER_EVENT_LOOP_THREADS);
      EventLoopGroups.warmUp(eventLoopGroup);
      serverBuilder.workerGroup(eventLoopGroup, true);
    }

    private void addCompression(ServerBuilder serverBuilder) {
      serverBuilder.decorator(EncodingService.builder().newDecorator());
    }

    private void setTimeout(ServerBuilder serverBuilder) {
      serverBuilder.requestTimeout(ARMERIA_TIMEOUT_DURATION);
      // todo - on timeout explore including a retry-after header via exception handler
      //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/503
      // sb.exceptionHandler()
    }

    private void addManagementEndpoints(ServerBuilder serverBuilder) {
      serverBuilder
          .service("/health", HealthCheckService.builder().build())
          .service("/metrics", (ctx, req) -> HttpResponse.of(prometheusMeterRegistry.scrape()))
          .serviceUnder("/docs", new DocService());
    }

    private void addTracing(ServerBuilder serverBuilder) {
      String endpoint = KaldbConfig.get().getTracingConfig().getZipkinEndpoint();
      SpanHandler spanHandler = new SpanHandler() {};

      if (!endpoint.isBlank()) {
        LOG.info(String.format("Trace reporting enabled: %s", endpoint));
        Sender sender = URLConnectionSender.create(endpoint);
        spanHandler = AsyncZipkinSpanHandler.create(sender);
      } else {
        LOG.info("Trace reporting disabled");
      }

      // always add a tracer, even if we're not reporting
      Tracing tracing =
          Tracing.newBuilder()
              .localServiceName(this.serviceName)
              .currentTraceContext(
                  RequestContextCurrentTraceContext.builder()
                      .addScopeDecorator(ThreadContextScopeDecorator.get())
                      .build())
              .addSpanHandler(spanHandler)
              .build();
      serverBuilder.decorator(BraveService.newDecorator(tracing));
    }

    private LoggingServiceBuilder getLoggingServiceBuilder() {
      return LoggingService.builder()
          // Not logging any successful response, say prom scraping /metrics every 30 seconds at
          // INFO
          .successfulResponseLogLevel(LogLevel.DEBUG)
          .failureResponseLogLevel(LogLevel.ERROR)
          // Remove the content to prevent blowing up the logs
          .responseContentSanitizer((ctx, content) -> "truncated")
          // Remove all headers to be sure we aren't leaking any auth/cookie info
          .requestHeadersSanitizer((ctx, headers) -> DefaultHttpHeaders.EMPTY_HEADERS);
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
    server.closeAsync().get(15, TimeUnit.SECONDS);
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
    return "ArmeriaService{" + "serviceName='" + serviceName + '\'' + '}';
  }
}
