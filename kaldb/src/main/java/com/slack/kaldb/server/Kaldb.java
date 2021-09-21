package com.slack.kaldb.server;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.slack.kaldb.chunk.manager.caching.CachingChunkManager;
import com.slack.kaldb.chunk.manager.indexing.ChunkCleanerService;
import com.slack.kaldb.chunk.manager.indexing.IndexingChunkManager;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbDistributedQueryService;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.util.RuntimeHalterImpl;
import com.slack.kaldb.writer.LogMessageTransformer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.kaldb.writer.kafka.KaldbKafkaWriter;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
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
  protected ServiceManager serviceManager;

  public Kaldb(Path configFilePath) throws IOException {
    Metrics.addRegistry(prometheusMeterRegistry);
    KaldbConfig.initFromFile(configFilePath);
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      LOG.info("Config file is needed a first argument");
    }
    Path configFilePath = Path.of(args[0]);

    Kaldb kalDb = new Kaldb(configFilePath);
    kalDb.start();
  }

  public void start() throws Exception {
    setupSystemMetrics();

    Set<Service> services = getServices();
    serviceManager = new ServiceManager(services);
    serviceManager.addListener(getServiceManagerListener(), MoreExecutors.directExecutor());
    addShutdownHook();

    serviceManager.startAsync();
  }

  public static Set<Service> getServices() throws Exception {
    Set<Service> services = new HashSet<>();

    HashSet<KaldbConfigs.NodeRole> roles = new HashSet<>(KaldbConfig.get().getNodeRolesList());

    MetadataStoreService metadataStoreService = new MetadataStoreService(prometheusMeterRegistry);
    services.add(metadataStoreService);

    if (roles.contains(KaldbConfigs.NodeRole.INDEX)) {
      IndexingChunkManager<LogMessage> chunkManager =
          IndexingChunkManager.fromConfig(
              prometheusMeterRegistry,
              metadataStoreService,
              KaldbConfig.get().getIndexerConfig().getServerConfig());
      services.add(chunkManager);

      ChunkCleanerService<LogMessage> chunkCleanerService =
          new ChunkCleanerService<>(
              chunkManager,
              Duration.ofSeconds(KaldbConfig.get().getIndexerConfig().getStaleDurationSecs()));
      services.add(chunkCleanerService);

      LogMessageTransformer messageTransformer = KaldbIndexer.getLogMessageTransformer();
      LogMessageWriterImpl logMessageWriterImpl =
          new LogMessageWriterImpl(chunkManager, messageTransformer);
      KaldbKafkaWriter kafkaWriter =
          KaldbKafkaWriter.fromConfig(logMessageWriterImpl, prometheusMeterRegistry);
      services.add(kafkaWriter);
      KaldbIndexer indexer = new KaldbIndexer(chunkManager, kafkaWriter);
      services.add(indexer);

      KaldbLocalQueryService<LogMessage> searcher =
          new KaldbLocalQueryService<>(indexer.getChunkManager());
      final int serverPort = KaldbConfig.get().getIndexerConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService(serverPort, prometheusMeterRegistry, searcher, "kalDbIndex");
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.QUERY)) {
      KaldbDistributedQueryService searcher = new KaldbDistributedQueryService();
      KaldbDistributedQueryService.servers =
          new ArrayList<>(KaldbConfig.get().getQueryConfig().getTmpIndexNodesList());
      final int serverPort = KaldbConfig.get().getQueryConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService(serverPort, prometheusMeterRegistry, searcher, "kalDbQuery");
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.CACHE)) {
      CachingChunkManager<LogMessage> chunkManager =
          new CachingChunkManager<>(
              prometheusMeterRegistry, metadataStoreService, KaldbConfig.get().getCacheConfig());
      CachingChunkManager.fromConfig(
          prometheusMeterRegistry, metadataStoreService, KaldbConfig.get().getCacheConfig());
      services.add(chunkManager);

      KaldbLocalQueryService<LogMessage> searcher = new KaldbLocalQueryService<>(chunkManager);
      final int serverPort = KaldbConfig.get().getCacheConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService(serverPort, prometheusMeterRegistry, searcher, "kalDbCache");
      services.add(armeriaService);
    }

    return services;
  }

  public static ServiceManager.Listener getServiceManagerListener() {
    return new ServiceManager.Listener() {
      @Override
      public void failure(Service service) {
        LOG.error(
            String.format(
                "Service %s failed with cause %s",
                service.getClass().toString(), service.failureCause().toString()));

        // shutdown if any services enters failure state
        new RuntimeHalterImpl()
            .handleFatal(new Throwable("Shutting down Kaldb due to failed service"));
      }
    };
  }

  public void shutdown() {
    try {
      serviceManager.stopAsync().awaitStopped(30, TimeUnit.SECONDS);

      // Ensure that log4j is the final thing to shut down, so that it available
      // throughout the service manager shutdown lifecycle
      if (LogManager.getContext() instanceof LoggerContext) {
        LOG.info("Shutting down log4j2");
        Configurator.shutdown((LoggerContext) LogManager.getContext());
      } else {
        LOG.error("Unable to shutdown log4j2");
      }
    } catch (TimeoutException timeout) {
      // stopping timed out
    }
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  private static void setupSystemMetrics() {
    // Expose JVM metrics.
    new ClassLoaderMetrics().bindTo(prometheusMeterRegistry);
    new JvmMemoryMetrics().bindTo(prometheusMeterRegistry);
    new JvmGcMetrics().bindTo(prometheusMeterRegistry);
    new ProcessorMetrics().bindTo(prometheusMeterRegistry);
    new JvmThreadMetrics().bindTo(prometheusMeterRegistry);

    LOG.info("Done registering standard JVM metrics for indexer service");
  }
}
