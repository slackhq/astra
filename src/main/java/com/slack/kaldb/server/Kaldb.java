package com.slack.kaldb.server;

import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.grpc.GrpcMeterIdPrefixFunction;
import com.linecorp.armeria.common.logging.LogLevel;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.logging.LoggingService;
import com.linecorp.armeria.server.logging.LoggingServiceBuilder;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class of Kaldb that sets up the basic infra needed for all the other end points like an a
 * http server, register monitoring libraries, create config manager etc..
 */
public class Kaldb {
  private static final Logger LOG = LoggerFactory.getLogger(Kaldb.class);

  private static final PrometheusMeterRegistry prometheusMeterRegistry =
      new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

  public Kaldb(Path configFilePath) throws IOException {
    Metrics.addRegistry(prometheusMeterRegistry);
    KaldbConfig.initFromFile(configFilePath);
  }

  public void setup() {
    LOG.info("Starting Kaldb server");

    setupMetrics();

    // Create an indexer and a grpc search service.
    GrpcServiceBuilder searchServiceBuilder = GrpcService.builder();
    KaldbIndexer indexer = KaldbIndexer.fromConfig(searchServiceBuilder, prometheusMeterRegistry);

    final int serverPort = KaldbConfig.get().getServerPort();
    // Create an API server to serve the search requests.
    ServerBuilder sb = Server.builder();
    sb.decorator(
        MetricCollectingService.newDecorator(GrpcMeterIdPrefixFunction.of("grpc.service")));
    sb.decorator(getLoggingServiceBuilder().newDecorator());
    sb.http(serverPort);
    sb.service("/health", HealthCheckService.builder().build());
    sb.service("/metrics", (ctx, req) -> HttpResponse.of(prometheusMeterRegistry.scrape()));
    sb.serviceUnder("/docs", new DocService());
    sb.service(searchServiceBuilder.build());
    Server server = sb.build();
    CompletableFuture<Void> serverFuture = server.start();
    serverFuture.join();
    LOG.info("Started server on port: {}", serverPort);

    // TODO: Instead of passing in the indexer, consider creating an interface or make indexer of
    // subclass of this class?
    // TODO: Start in a background thread.
    indexer.start();

    // TODO: On CTRL-C shut down the process cleanly. Ensure no write locks in indexer. Guava
    // ServiceManager?
  }

  private LoggingServiceBuilder getLoggingServiceBuilder() {
    return LoggingService.builder()
        .successfulResponseLogLevel(LogLevel.INFO)
        .failureResponseLogLevel(LogLevel.ERROR)
        .requestHeadersSanitizer((ctx, headers) -> {
          if (headers.contains(HttpHeaderNames.AUTHORIZATION)) {
            headers =
                headers
                    .toBuilder()
                    .removeAndThen(HttpHeaderNames.AUTHORIZATION)
                    .add(HttpHeaderNames.AUTHORIZATION, "present_but_scrubbed")
                    .build();
          }
          return headers;
        });
  }


  private void setupMetrics() {
    // Expose JVM metrics.
    new ClassLoaderMetrics().bindTo(prometheusMeterRegistry);
    new JvmMemoryMetrics().bindTo(prometheusMeterRegistry);
    new JvmGcMetrics().bindTo(prometheusMeterRegistry);
    new ProcessorMetrics().bindTo(prometheusMeterRegistry);
    new JvmThreadMetrics().bindTo(prometheusMeterRegistry);
    LOG.info("Done registering standard JVM metrics.");
  }

  public void close() {
    LOG.info("Shutting down Kaldb server");
    // TODO: Add a on exit method handler for the serve?
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      LOG.info("Config file is needed a first argument");
    }
    Path configFilePath = Path.of(args[0]);

    Kaldb kalDb = new Kaldb(configFilePath);
    kalDb.setup();
  }
}
