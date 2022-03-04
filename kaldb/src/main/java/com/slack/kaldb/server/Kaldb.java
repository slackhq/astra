package com.slack.kaldb.server;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunkManager.CachingChunkManager;
import com.slack.kaldb.chunkManager.ChunkCleanerService;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.clusterManager.RecoveryTaskAssignmentService;
import com.slack.kaldb.clusterManager.ReplicaAssignmentService;
import com.slack.kaldb.clusterManager.ReplicaCreationService;
import com.slack.kaldb.clusterManager.ReplicaDeletionService;
import com.slack.kaldb.clusterManager.ReplicaEvictionService;
import com.slack.kaldb.clusterManager.SnapshotDeletionService;
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
import org.apache.logging.log4j.LogManager;
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
  private final KaldbConfigs.KaldbConfig kaldbConfig;
  protected ServiceManager serviceManager;
  protected MetadataStore metadataStore;

  public Kaldb(KaldbConfigs.KaldbConfig kaldbConfig) throws IOException {
    Metrics.addRegistry(prometheusMeterRegistry);
    this.kaldbConfig = kaldbConfig;
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      LOG.info("Config file is needed a first argument");
    }
    Path configFilePath = Path.of(args[0]);

    KaldbConfig.initFromFile(configFilePath);
    Kaldb kalDb = new Kaldb(KaldbConfig.get());
    kalDb.start();
  }

  public void start() throws Exception {
    setupSystemMetrics();

    metadataStore =
        ZookeeperMetadataStoreImpl.fromConfig(
            prometheusMeterRegistry, kaldbConfig.getMetadataStoreConfig().getZookeeperConfig());

    Set<Service> services = getServices(metadataStore, kaldbConfig);
    serviceManager = new ServiceManager(services);
    serviceManager.addListener(getServiceManagerListener(), MoreExecutors.directExecutor());
    addShutdownHook();

    serviceManager.startAsync();
  }

  public static Set<Service> getServices(
      MetadataStore metadataStore, KaldbConfigs.KaldbConfig kaldbConfig) throws Exception {
    Set<Service> services = new HashSet<>();

    HashSet<KaldbConfigs.NodeRole> roles = new HashSet<>(kaldbConfig.getNodeRolesList());

    if (roles.contains(KaldbConfigs.NodeRole.INDEX)) {
      IndexingChunkManager<LogMessage> chunkManager =
          IndexingChunkManager.fromConfig(
              prometheusMeterRegistry,
              metadataStore,
              kaldbConfig.getIndexerConfig(),
              kaldbConfig.getS3Config());
      services.add(chunkManager);

      ChunkCleanerService<LogMessage> chunkCleanerService =
          new ChunkCleanerService<>(
              chunkManager,
              Duration.ofSeconds(kaldbConfig.getIndexerConfig().getStaleDurationSecs()));
      services.add(chunkCleanerService);

      KaldbIndexer indexer =
          new KaldbIndexer(
              chunkManager,
              metadataStore,
              kaldbConfig.getIndexerConfig(),
              kaldbConfig.getKafkaConfig(),
              prometheusMeterRegistry);
      services.add(indexer);

      KaldbLocalQueryService<LogMessage> searcher = new KaldbLocalQueryService<>(chunkManager);
      final int serverPort = kaldbConfig.getIndexerConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbIndex", prometheusMeterRegistry)
              .withTracingEndpoint(kaldbConfig.getTracingConfig().getZipkinEndpoint())
              .withGrpcSearchApi(searcher)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.QUERY)) {
      SearchMetadataStore searchMetadataStore = new SearchMetadataStore(metadataStore, true);
      services.add(
          new MetadataStoreLifecycleManager(
              KaldbConfigs.NodeRole.QUERY, Collections.singletonList(searchMetadataStore)));

      KaldbDistributedQueryService kaldbDistributedQueryService =
          new KaldbDistributedQueryService(searchMetadataStore, prometheusMeterRegistry);
      final int serverPort = kaldbConfig.getQueryConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbQuery", prometheusMeterRegistry)
              .withTracingEndpoint(kaldbConfig.getTracingConfig().getZipkinEndpoint())
              .withElasticsearchApi(kaldbDistributedQueryService)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.CACHE)) {
      CachingChunkManager<LogMessage> chunkManager =
          CachingChunkManager.fromConfig(
              prometheusMeterRegistry,
              metadataStore,
              kaldbConfig.getS3Config(),
              kaldbConfig.getCacheConfig());
      services.add(chunkManager);

      KaldbLocalQueryService<LogMessage> searcher = new KaldbLocalQueryService<>(chunkManager);
      final int serverPort = kaldbConfig.getCacheConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbCache", prometheusMeterRegistry)
              .withTracingEndpoint(kaldbConfig.getTracingConfig().getZipkinEndpoint())
              .withGrpcSearchApi(searcher)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.MANAGER)) {
      final KaldbConfigs.ManagerConfig managerConfig = kaldbConfig.getManagerConfig();
      final int serverPort = managerConfig.getServerConfig().getServerPort();

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbManager", prometheusMeterRegistry)
              .withTracingEndpoint(kaldbConfig.getTracingConfig().getZipkinEndpoint())
              .build();
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

      ReplicaEvictionService replicaEvictionService =
          new ReplicaEvictionService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, prometheusMeterRegistry);
      services.add(replicaEvictionService);

      ReplicaDeletionService replicaDeletionService =
          new ReplicaDeletionService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, prometheusMeterRegistry);
      services.add(replicaDeletionService);

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

      S3BlobFs s3BlobFs = S3BlobFs.getS3BlobFsClient(kaldbConfig.getS3Config());
      SnapshotDeletionService snapshotDeletionService =
          new SnapshotDeletionService(
              replicaMetadataStore,
              snapshotMetadataStore,
              s3BlobFs,
              managerConfig,
              prometheusMeterRegistry);
      services.add(snapshotDeletionService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.RECOVERY)) {
      final KaldbConfigs.RecoveryConfig recoveryConfig = kaldbConfig.getRecoveryConfig();
      final int serverPort = recoveryConfig.getServerConfig().getServerPort();

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbRecovery", prometheusMeterRegistry)
              .withTracingEndpoint(kaldbConfig.getTracingConfig().getZipkinEndpoint())
              .build();
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
    } catch (Exception e) {
      // stopping timed out
      LOG.error("ServiceManager shutdown timed out", e);
    }
    try {
      metadataStore.close();
    } catch (Exception e) {
      LOG.error("Error while calling metadataStore.close() ", e);
    }
    LOG.info("Shutting down LogManager");
    LogManager.shutdown();
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
