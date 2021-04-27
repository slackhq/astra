package com.slack.kaldb.server;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.slack.kaldb.config.KaldbConfig;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class of Kaldb that sets up the basic infra needed for all the other end points like an a
 * http server, register monitoring libraries, create config manager etc..
 */
public class Kaldb {
  private static final Logger LOG = LoggerFactory.getLogger(Kaldb.class);

  private final SimpleMeterRegistry meterRegistry;

  public Kaldb(Path configFilePath) throws IOException {
    LOG.info("Starting Metrics setup.");
    // TODO: Initialize metrics and set up a metrics end point.
    // Metrics.addRegistry(Metrics.globalRegistry);
    meterRegistry = new SimpleMeterRegistry();
    Metrics.addRegistry(meterRegistry);
    // TODO: Expose a metrics scraper endpoint.
    LOG.info("Finished metrics setup.");

    KaldbConfig.initFromFile(configFilePath);
  }

  public void setup() {
    LOG.info("Starting Kaldb server");

    setupMetrics();

    // Create an indexer and a grpc search service.
    GrpcServiceBuilder searchServiceBuilder = GrpcService.builder();
    KaldbIndexer indexer = KaldbIndexer.fromConfig(searchServiceBuilder, meterRegistry);

    // Create an API server to serve the search requests.
    ServerBuilder sb = Server.builder();
    sb.http(8080);
    sb.service("/ping", (ctx, req) -> HttpResponse.of("pong!"));
    sb.service(searchServiceBuilder.build());
    Server server = sb.build();
    CompletableFuture<Void> serverFuture = server.start();
    serverFuture.join();

    // TODO: Instead of passing in the indexer, consider creating an interface or make indexer of
    // subclass of this class?
    // TODO: Start in a background thread.
    indexer.start();

    // TODO: On CTRL-C shut down the process cleanly. Ensure no write locks in indexer. Guava
    // ServiceManager?
  }

  private void setupMetrics() {
    // TODO: Setup prom metrics.
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
