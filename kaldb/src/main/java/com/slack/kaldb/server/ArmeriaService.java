package com.slack.kaldb.server;

import static com.slack.kaldb.server.KaldbConfig.ARMERIA_TIMEOUT_DURATION;

import brave.Tracing;
import brave.context.log4j2.ThreadContextScopeDecorator;
import brave.handler.SpanHandler;
import com.google.common.util.concurrent.AbstractIdleService;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.brave.RequestContextCurrentTraceContext;
import com.linecorp.armeria.common.grpc.GrpcMeterIdPrefixFunction;
import com.linecorp.armeria.common.logging.LogLevel;
import com.linecorp.armeria.common.util.EventLoopGroups;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.brave.BraveService;
import com.linecorp.armeria.server.docs.DocService;
import com.linecorp.armeria.server.encoding.EncodingService;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.logging.LoggingService;
import com.linecorp.armeria.server.management.ManagementService;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import io.grpc.BindableService;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.reporter.Sender;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

public class ArmeriaService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ArmeriaService.class);

  private static final int WORKER_EVENT_LOOP_THREADS = 16;

  private final String serviceName;
  private final Server appServer;
  private final Server adminServer;

  private ArmeriaService(Server appServer, Server adminServer, String serviceName) {
    this.appServer = appServer;
    this.adminServer = adminServer;
    this.serviceName = serviceName;
  }

  public static class Builder {
    private final String serviceName;
    private final ServerBuilder appServerBuilder;
    private final ServerBuilder adminServerBuilder;
    private SpanHandler spanHandler = new SpanHandler() {};

    public Builder(
        int appPort,
        int adminPort,
        String serviceName,
        PrometheusMeterRegistry prometheusMeterRegistry) {
      this.serviceName = serviceName;
      this.appServerBuilder = Server.builder().http(appPort);
      this.adminServerBuilder = Server.builder().http(adminPort);

      initializeEventLoop();
      initializeTimeouts();
      initializeCompression();
      initializeLogging();
      initializeManagementEndpoints(prometheusMeterRegistry);
    }

    private void initializeEventLoop() {
      EventLoopGroup eventLoopGroup = EventLoopGroups.newEventLoopGroup(WORKER_EVENT_LOOP_THREADS);
      EventLoopGroups.warmUp(eventLoopGroup);
      appServerBuilder.workerGroup(eventLoopGroup, true);
    }

    private void initializeTimeouts() {
      appServerBuilder.requestTimeout(ARMERIA_TIMEOUT_DURATION);
      adminServerBuilder.requestTimeout(ARMERIA_TIMEOUT_DURATION);
    }

    private void initializeCompression() {
      appServerBuilder.decorator(EncodingService.builder().newDecorator());
    }

    private void initializeLogging() {
      Function<? super HttpService, LoggingService> loggingDecorator =
          LoggingService.builder()
              // Not logging any successful response
              .successfulResponseLogLevel(LogLevel.DEBUG)
              .failureResponseLogLevel(LogLevel.ERROR)
              // Remove the content to prevent blowing up the logs
              .responseContentSanitizer((ctx, content) -> "truncated")
              // Remove all headers to be sure we aren't leaking any auth/cookie info
              .requestHeadersSanitizer((ctx, headers) -> DefaultHttpHeaders.EMPTY_HEADERS)
              .newDecorator();

      appServerBuilder.decorator(loggingDecorator);
      adminServerBuilder.decorator(loggingDecorator);
    }

    private void initializeManagementEndpoints(PrometheusMeterRegistry prometheusMeterRegistry) {
      appServerBuilder.service("/ping", (ctx, req) -> HttpResponse.of("pong"));
      adminServerBuilder
          .service("/ping", (ctx, req) -> HttpResponse.of("pong"))
          .service("/health", HealthCheckService.builder().build())
          .service("/metrics", (ctx, req) -> HttpResponse.of(prometheusMeterRegistry.scrape()))
          .service("/management", ManagementService.of())
          .serviceUnder("/docs", new DocService());
    }

    public Builder withTracingEndpoint(String endpoint) {
      if (endpoint != null && !endpoint.isBlank()) {
        LOG.info(String.format("Trace reporting enabled: %s", endpoint));
        Sender sender = URLConnectionSender.create(endpoint);
        spanHandler = AsyncZipkinSpanHandler.create(sender);
      }
      return this;
    }

    public Builder withAnnotatedService(Object service) {
      appServerBuilder.annotatedService(service);
      return this;
    }

    public Builder withGrpcService(BindableService grpcService) {
      GrpcServiceBuilder searchBuilder =
          GrpcService.builder()
              .addService(grpcService)
              .enableUnframedRequests(true)
              .useBlockingTaskExecutor(true);
      appServerBuilder.decorator(
          MetricCollectingService.newDecorator(GrpcMeterIdPrefixFunction.of("grpc.service")));
      appServerBuilder.service(searchBuilder.build());
      return this;
    }

    public ArmeriaService build() {
      // always add tracing, even if it's not being reported to an endpoint
      appServerBuilder.decorator(
          BraveService.newDecorator(
              Tracing.newBuilder()
                  .localServiceName(serviceName)
                  .currentTraceContext(
                      RequestContextCurrentTraceContext.builder()
                          .addScopeDecorator(ThreadContextScopeDecorator.get())
                          .build())
                  .addSpanHandler(spanHandler)
                  .build()));

      return new ArmeriaService(appServerBuilder.build(), adminServerBuilder.build(), serviceName);
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting appServer for {} on ports {}", serviceName, appServer.config().ports());
    CompletableFuture<Void> appFuture = appServer.start();

    LOG.info("Starting adminServer for {} on ports {}", serviceName, adminServer.config().ports());
    CompletableFuture<Void> adminFuture = adminServer.start();

    appFuture.get(15, TimeUnit.SECONDS);
    adminFuture.get(15, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    // On server close there is an option for a graceful shutdown, which is disabled by default.
    // When it is disabled it immediately starts rejecting requests and begins the shutdown
    // process, which includes running any remaining AsyncClosableSupport closeAsync actions. We
    // want to allow this to take up to the maximum permissible shutdown time to successfully close.

    LOG.info("Shutting down appServer for {}", serviceName);
    appServer.close();

    LOG.info("Shutting down adminServer for {}", serviceName);
    adminServer.close();
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
