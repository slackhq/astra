package com.slack.kaldb.server;

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
import com.linecorp.armeria.server.management.ManagementService;
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

  private static final int WORKER_EVENT_LOOP_THREADS = 16;

  private final PrometheusMeterRegistry prometheusMeterRegistry;
  private final String serviceName;
  private final Server server;
  private final int serverPort;

  public ArmeriaService(
      int serverPort, PrometheusMeterRegistry prometheusMeterRegistry, String serviceName) {
    this.serverPort = serverPort;
    this.prometheusMeterRegistry = prometheusMeterRegistry;
    this.serviceName = serviceName;

    ServerBuilder sb = Server.builder();
    initializeEventLoop(sb);
    addCompression(sb);
    addManagementEndpoints(sb);
    addTracing(sb);
    this.server = sb.build();
  }

  public ArmeriaService(
      int serverPort,
      PrometheusMeterRegistry prometheusMeterRegistry,
      KaldbServiceGrpc.KaldbServiceImplBase searcher,
      String serviceName) {
    this.serverPort = serverPort;
    this.prometheusMeterRegistry = prometheusMeterRegistry;
    this.serviceName = serviceName;

    ServerBuilder sb = Server.builder();
    addSearchServices(sb, searcher);
    initializeEventLoop(sb);
    addCompression(sb);
    addManagementEndpoints(sb);
    addTracing(sb);
    this.server = sb.build();
  }

  private void addSearchServices(ServerBuilder sb, KaldbServiceGrpc.KaldbServiceImplBase searcher) {
    GrpcServiceBuilder searchBuilder =
        GrpcService.builder()
            .addService(searcher)
            .enableUnframedRequests(true)
            .useBlockingTaskExecutor(true);
    sb.service(searchBuilder.build());
    sb.annotatedService(new ElasticsearchApiService(searcher));
  }

  private void initializeEventLoop(ServerBuilder sb) {
    EventLoopGroup eventLoopGroup = EventLoopGroups.newEventLoopGroup(WORKER_EVENT_LOOP_THREADS);
    EventLoopGroups.warmUp(eventLoopGroup);
    sb.workerGroup(eventLoopGroup, true);
  }

  private void addCompression(ServerBuilder sb) {
    sb.decorator(EncodingService.builder().newDecorator());
  }

  private void addTracing(ServerBuilder sb) {
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
    sb.decorator(BraveService.newDecorator(tracing));
  }

  private void addManagementEndpoints(ServerBuilder sb) {
    sb.decorator(
        MetricCollectingService.newDecorator(GrpcMeterIdPrefixFunction.of("grpc.service")));
    sb.decorator(getLoggingServiceBuilder().newDecorator());
    sb.http(serverPort);
    sb.service("/health", HealthCheckService.builder().build());
    sb.service("/metrics", (ctx, req) -> HttpResponse.of(prometheusMeterRegistry.scrape()));
    sb.serviceUnder("/docs", new DocService());
    sb.serviceUnder("/internal/management/", ManagementService.of());
  }

  private LoggingServiceBuilder getLoggingServiceBuilder() {
    return LoggingService.builder()
        // Not logging any successful response, say prom scraping /metrics every 30 seconds at INFO
        .successfulResponseLogLevel(LogLevel.DEBUG)
        .failureResponseLogLevel(LogLevel.ERROR)
        // Remove the content to prevent blowing up the logs
        .responseContentSanitizer((ctx, content) -> "truncated")
        // Remove all headers to be sure we aren't leaking any auth/cookie info
        .requestHeadersSanitizer((ctx, headers) -> DefaultHttpHeaders.EMPTY_HEADERS);
  }

  @Override
  protected void startUp() throws Exception {
    CompletableFuture<Void> serverFuture = server.start();
    serverFuture.get(15, TimeUnit.SECONDS);
    LOG.info("Started on port: {}", serverPort);
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
}
