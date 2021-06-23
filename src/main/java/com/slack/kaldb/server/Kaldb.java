package com.slack.kaldb.server;

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
import com.linecorp.armeria.server.management.ManagementService;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import com.slack.kaldb.config.KaldbConfig;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class of Kaldb that sets up the basic infra needed for all the other end points like an a
 * http server, register monitoring libraries, create config manager etc..
 */
public class Kaldb {
  private static final Logger LOG = LoggerFactory.getLogger(Kaldb.class);

  public static String READ_NODE_ROLE = "read";
  public static String CACHE_NODE_ROLE = "cache";
  public static String INDEX_NODE_ROLE = "index";

  private static final PrometheusMeterRegistry indexerPromMeterRegistry =
      new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

  private static final PrometheusMeterRegistry readPromMeterRegistry =
      new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

  public Kaldb(Path configFilePath) throws IOException {
    Metrics.addRegistry(indexerPromMeterRegistry);
    Metrics.addRegistry(readPromMeterRegistry);
    KaldbConfig.initFromFile(configFilePath);
  }

  private void addManagementEndpoints(
      ServerBuilder sb, int serverPort, PrometheusMeterRegistry meterRegistry) {
    sb.decorator(
        MetricCollectingService.newDecorator(GrpcMeterIdPrefixFunction.of("grpc.service")));
    sb.decorator(getLoggingServiceBuilder().newDecorator());
    sb.http(serverPort);
    sb.service("/health", HealthCheckService.builder().build());
    sb.service("/metrics", (ctx, req) -> HttpResponse.of(meterRegistry.scrape()));
    sb.serviceUnder("/docs", new DocService());
    sb.serviceUnder("/internal/management/", ManagementService.of());
  }

  public void setup() {
    LOG.info("Starting Kaldb server");
    HashSet<String> roles = new HashSet<>(KaldbConfig.get().getNodeRolesList());

    if (roles.contains(INDEX_NODE_ROLE)) {
      setupMetrics(indexerPromMeterRegistry);
      LOG.info("Done registering standard JVM metrics for indexer service");

      ServerBuilder sb = Server.builder();
      // Create an indexer and a grpc search service.
      KaldbIndexer indexer = KaldbIndexer.fromConfig(indexerPromMeterRegistry);

      // Create a protobuf handler service that calls chunkManager on search.
      GrpcServiceBuilder searchBuilder =
          GrpcService.builder()
              .addService(new KaldbLocalSearcher<>(indexer.getChunkManager()))
              .enableUnframedRequests(true);
      sb.service(searchBuilder.build());

      final int serverPort = KaldbConfig.get().getIndexerConfig().getServerPort();
      addManagementEndpoints(sb, serverPort, indexerPromMeterRegistry);

      Server server = sb.build();

      CompletableFuture<Void> serverFuture = server.start();
      serverFuture.join();
      LOG.info("Started indexer server on port: {}", serverPort);

      // TODO: Instead of passing in the indexer, consider creating an interface or make indexer of
      // subclass of this class?
      // TODO: Start in a background thread.
      indexer.start();

      // TODO: On CTRL-C shut down the process cleanly. Ensure no write locks in indexer. Guava
      // ServiceManager?
    }

    if (roles.contains(READ_NODE_ROLE)) {
      setupMetrics(readPromMeterRegistry);
      LOG.info("Done registering standard JVM metrics for read service");

      ServerBuilder sb = Server.builder();

      GrpcServiceBuilder searchBuilder =
          GrpcService.builder().addService(new KalDBReadService()).enableUnframedRequests(true);
      sb.service(searchBuilder.build());

      final int serverPort = KaldbConfig.get().getReadConfig().getServerPort();
      addManagementEndpoints(sb, serverPort, readPromMeterRegistry);

      Server server = sb.build();

      CompletableFuture<Void> serverFuture = server.start();
      serverFuture.join();
      LOG.info("Started read server on port: {}", serverPort);
    }
  }

  private LoggingServiceBuilder getLoggingServiceBuilder() {
    return LoggingService.builder()
        // Not logging any successful response, say prom scraping /metrics every 30 seconds at INFO
        .successfulResponseLogLevel(LogLevel.DEBUG)
        .failureResponseLogLevel(LogLevel.ERROR)
        // Remove all headers to be sure we aren't leaking any auth/cookie info
        .requestHeadersSanitizer((ctx, headers) -> DefaultHttpHeaders.EMPTY_HEADERS);
  }

  private void setupMetrics(PrometheusMeterRegistry meterRegistry) {
    // Expose JVM metrics.
    new ClassLoaderMetrics().bindTo(meterRegistry);
    new JvmMemoryMetrics().bindTo(meterRegistry);
    new JvmGcMetrics().bindTo(meterRegistry);
    new ProcessorMetrics().bindTo(meterRegistry);
    new JvmThreadMetrics().bindTo(meterRegistry);
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
