package com.slack.kaldb.server;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.slack.kaldb.chunkManager.CachingChunkManager;
import com.slack.kaldb.chunkManager.ChunkCleanerService;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.clusterManager.RecoveryTaskAssignmentService;
import com.slack.kaldb.clusterManager.ReplicaAssignmentService;
import com.slack.kaldb.clusterManager.ReplicaCreationService;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbDistributedQueryService;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStoreLifecycleManager;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.recovery.RecoveryService;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
  protected MetadataStore metadataStore;

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

    metadataStore =
        ZookeeperMetadataStoreImpl.fromConfig(
            prometheusMeterRegistry,
            KaldbConfig.get().getMetadataStoreConfig().getZookeeperConfig());

    Set<Service> services = getServices(metadataStore);
    serviceManager = new ServiceManager(services);
    serviceManager.addListener(getServiceManagerListener(), MoreExecutors.directExecutor());
    addShutdownHook();

    serviceManager.startAsync();
  }

  public static Set<Service> getServices(MetadataStore metadataStore) throws Exception {
    Set<Service> services = new HashSet<>();

    HashSet<KaldbConfigs.NodeRole> roles = new HashSet<>(KaldbConfig.get().getNodeRolesList());

    if (roles.contains(KaldbConfigs.NodeRole.INDEX)) {
      IndexingChunkManager<LogMessage> chunkManager =
          IndexingChunkManager.fromConfig(
              prometheusMeterRegistry,
              metadataStore,
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
      SearchMetadataStore searchMetadataStore = new SearchMetadataStore(metadataStore, true);
      services.add(
          new MetadataStoreLifecycleManager(
              KaldbConfigs.NodeRole.QUERY, Collections.singletonList(searchMetadataStore)));

      KaldbDistributedQueryService kaldbDistributedQueryService =
          new KaldbDistributedQueryService(searchMetadataStore, prometheusMeterRegistry);
      final int serverPort = KaldbConfig.get().getQueryConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService(
              serverPort, prometheusMeterRegistry, kaldbDistributedQueryService, "kalDbQuery");
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.CACHE)) {
      CachingChunkManager<LogMessage> chunkManager =
          CachingChunkManager.fromConfig(
              prometheusMeterRegistry,
              metadataStore,
              KaldbConfig.get().getCacheConfig().getServerConfig());
      services.add(chunkManager);

      KaldbLocalQueryService<LogMessage> searcher = new KaldbLocalQueryService<>(chunkManager);
      final int serverPort = KaldbConfig.get().getCacheConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService(serverPort, prometheusMeterRegistry, searcher, "kalDbCache");
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.MANAGER)) {
      final KaldbConfigs.ManagerConfig managerConfig = KaldbConfig.get().getManagerConfig();
      final int serverPort = managerConfig.getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService(serverPort, prometheusMeterRegistry, "kalDbManager");
      services.add(armeriaService);

      ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(metadataStore, true);
      SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, true);
      RecoveryTaskMetadataStore recoveryTaskMetadataStore =
          new RecoveryTaskMetadataStore(metadataStore, true);
      RecoveryNodeMetadataStore recoveryNodeMetadataStore =
          new RecoveryNodeMetadataStore(metadataStore, true);
      CacheSlotMetadataStore cacheSlotMetadataStore =
          new CacheSlotMetadataStore(metadataStore, true);

      services.add(
          new MetadataStoreLifecycleManager(
              KaldbConfigs.NodeRole.MANAGER,
              List.of(
                  replicaMetadataStore,
                  snapshotMetadataStore,
                  recoveryTaskMetadataStore,
                  recoveryNodeMetadataStore,
                  cacheSlotMetadataStore)));

      ReplicaCreationService replicaCreationService =
          new ReplicaCreationService(
              replicaMetadataStore, snapshotMetadataStore, managerConfig, prometheusMeterRegistry);
      services.add(replicaCreationService);

      RecoveryTaskAssignmentService recoveryTaskAssignmentService =
          new RecoveryTaskAssignmentService(
              recoveryTaskMetadataStore,
              recoveryNodeMetadataStore,
              managerConfig,
              prometheusMeterRegistry);
      services.add(recoveryTaskAssignmentService);

      ReplicaAssignmentService replicaAssignmentService =
          new ReplicaAssignmentService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, prometheusMeterRegistry);
      services.add(replicaAssignmentService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.RECOVERY)) {
      final KaldbConfigs.RecoveryConfig recoveryConfig = KaldbConfig.get().getRecoveryConfig();
      final int serverPort = recoveryConfig.getServerConfig().getServerPort();

      ArmeriaService armeriaService =
          new ArmeriaService(serverPort, prometheusMeterRegistry, "kaldbRecovery");
      services.add(armeriaService);

      RecoveryService recoveryService =
          new RecoveryService(recoveryConfig, metadataStore, prometheusMeterRegistry);
      services.add(recoveryService);
    }

    return services;
  }

  public static ServiceManager.Listener getServiceManagerListener() {
    return new ServiceManager.Listener() {
      @Override
      public void failure(Service service) {
        LOG.error(
            String.format("Service %s failed with cause ", service.getClass().toString()),
            service.failureCause());

        // shutdown if any services enters failure state
        new RuntimeHalterImpl()
            .handleFatal(new Throwable("Shutting down Kaldb due to failed service"));
      }
    };
  }

  public void shutdown() {
    try {
      serviceManager.stopAsync().awaitStopped(30, TimeUnit.SECONDS);
      metadataStore.close();

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
