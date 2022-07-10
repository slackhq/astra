package com.slack.kaldb.server;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.slack.kaldb.blobfs.BlobFs;
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
import com.slack.kaldb.elasticsearchApi.ElasticsearchApiService;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbDistributedQueryService;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.service.ServiceMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStoreLifecycleManager;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.preprocessor.PreprocessorService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.recovery.RecoveryService;
import com.slack.kaldb.util.RuntimeHalterImpl;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Main class of Kaldb that sets up the basic infra needed for all the other end points like an a
 * http server, register monitoring libraries, create config manager etc..
 */
public class Kaldb {
  private static final Logger LOG = LoggerFactory.getLogger(Kaldb.class);

  private final PrometheusMeterRegistry prometheusMeterRegistry;

  private final KaldbConfigs.KaldbConfig kaldbConfig;
  private final S3Client s3Client;
  protected ServiceManager serviceManager;
  protected MetadataStore metadataStore;

  Kaldb(
      KaldbConfigs.KaldbConfig kaldbConfig,
      S3Client s3Client,
      PrometheusMeterRegistry prometheusMeterRegistry) {
    this.prometheusMeterRegistry = prometheusMeterRegistry;
    this.kaldbConfig = kaldbConfig;
    this.s3Client = s3Client;
    Metrics.addRegistry(prometheusMeterRegistry);
    LOG.info("Started Kaldb process with config: {}", kaldbConfig);
  }

  Kaldb(KaldbConfigs.KaldbConfig kaldbConfig, PrometheusMeterRegistry prometheusMeterRegistry) {
    this(kaldbConfig, S3BlobFs.initS3Client(kaldbConfig.getS3Config()), prometheusMeterRegistry);
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      LOG.info("Config file is needed a first argument");
    }
    Path configFilePath = Path.of(args[0]);

    KaldbConfig.initFromFile(configFilePath);
    KaldbConfigs.KaldbConfig config = KaldbConfig.get();
    Kaldb kalDb = new Kaldb(KaldbConfig.get(), initPrometheusMeterRegistry(config));
    kalDb.start();
  }

  static PrometheusMeterRegistry initPrometheusMeterRegistry(KaldbConfigs.KaldbConfig config) {
    PrometheusMeterRegistry prometheusMeterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    prometheusMeterRegistry
        .config()
        .commonTags(
            "kaldb_cluster_name",
            config.getClusterConfig().getClusterName(),
            "kaldb_env",
            config.getClusterConfig().getEnv(),
            "kaldb_component",
            getComponentTag(config));
    return prometheusMeterRegistry;
  }

  private static String getComponentTag(KaldbConfigs.KaldbConfig config) {
    String component;
    if (config.getNodeRolesList().size() == 1) {
      component = config.getNodeRolesList().get(0).toString();
    } else {
      component = Strings.join(config.getNodeRolesList(), '-');
    }
    return Strings.toRootLowerCase(component);
  }

  public void start() throws Exception {
    setupSystemMetrics(prometheusMeterRegistry);
    addShutdownHook();

    metadataStore =
        ZookeeperMetadataStoreImpl.fromConfig(
            prometheusMeterRegistry, kaldbConfig.getMetadataStoreConfig().getZookeeperConfig());

    // Initialize blobfs. Only S3 is supported currently.
    S3BlobFs s3BlobFs = new S3BlobFs(s3Client);

    Set<Service> services =
        getServices(metadataStore, kaldbConfig, s3BlobFs, prometheusMeterRegistry);
    serviceManager = new ServiceManager(services);
    serviceManager.addListener(getServiceManagerListener(), MoreExecutors.directExecutor());

    serviceManager.startAsync();
  }

  private static Set<Service> getServices(
      MetadataStore metadataStore,
      KaldbConfigs.KaldbConfig kaldbConfig,
      BlobFs blobFs,
      PrometheusMeterRegistry meterRegistry)
      throws Exception {
    Set<Service> services = new HashSet<>();

    HashSet<KaldbConfigs.NodeRole> roles = new HashSet<>(kaldbConfig.getNodeRolesList());

    if (roles.contains(KaldbConfigs.NodeRole.INDEX)) {
      IndexingChunkManager<LogMessage> chunkManager =
          IndexingChunkManager.fromConfig(
              meterRegistry,
              metadataStore,
              kaldbConfig.getIndexerConfig(),
              blobFs,
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
              meterRegistry);
      services.add(indexer);

      KaldbLocalQueryService<LogMessage> searcher = new KaldbLocalQueryService<>(chunkManager);
      final int serverPort = kaldbConfig.getIndexerConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbIndex", meterRegistry)
              .withTracing(kaldbConfig.getTracingConfig())
              .withGrpcService(searcher)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.QUERY)) {
      SearchMetadataStore searchMetadataStore = new SearchMetadataStore(metadataStore, true);
      SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, true);
      ServiceMetadataStore serviceMetadataStore = new ServiceMetadataStore(metadataStore, true);
      services.add(
          new MetadataStoreLifecycleManager(
              KaldbConfigs.NodeRole.QUERY, List.of(searchMetadataStore, snapshotMetadataStore)));

      KaldbDistributedQueryService kaldbDistributedQueryService =
          new KaldbDistributedQueryService(
              searchMetadataStore, snapshotMetadataStore, serviceMetadataStore, meterRegistry);
      final int serverPort = kaldbConfig.getQueryConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbQuery", meterRegistry)
              .withTracing(kaldbConfig.getTracingConfig())
              .withAnnotatedService(new ElasticsearchApiService(kaldbDistributedQueryService))
              .withGrpcService(kaldbDistributedQueryService)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.CACHE)) {
      CachingChunkManager<LogMessage> chunkManager =
          CachingChunkManager.fromConfig(
              meterRegistry,
              metadataStore,
              kaldbConfig.getS3Config(),
              kaldbConfig.getCacheConfig(),
              blobFs);
      services.add(chunkManager);

      KaldbLocalQueryService<LogMessage> searcher = new KaldbLocalQueryService<>(chunkManager);
      final int serverPort = kaldbConfig.getCacheConfig().getServerConfig().getServerPort();
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbCache", meterRegistry)
              .withTracing(kaldbConfig.getTracingConfig())
              .withGrpcService(searcher)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.MANAGER)) {
      final KaldbConfigs.ManagerConfig managerConfig = kaldbConfig.getManagerConfig();
      final int serverPort = managerConfig.getServerConfig().getServerPort();

      ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(metadataStore, true);
      SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, true);
      RecoveryTaskMetadataStore recoveryTaskMetadataStore =
          new RecoveryTaskMetadataStore(metadataStore, true);
      RecoveryNodeMetadataStore recoveryNodeMetadataStore =
          new RecoveryNodeMetadataStore(metadataStore, true);
      CacheSlotMetadataStore cacheSlotMetadataStore =
          new CacheSlotMetadataStore(metadataStore, true);
      ServiceMetadataStore serviceMetadataStore = new ServiceMetadataStore(metadataStore, true);

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbManager", meterRegistry)
              .withTracing(kaldbConfig.getTracingConfig())
              .withGrpcService(new ManagerApiGrpc(serviceMetadataStore))
              .build();
      services.add(armeriaService);

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
              replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
      services.add(replicaCreationService);

      ReplicaEvictionService replicaEvictionService =
          new ReplicaEvictionService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
      services.add(replicaEvictionService);

      ReplicaDeletionService replicaDeletionService =
          new ReplicaDeletionService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
      services.add(replicaDeletionService);

      RecoveryTaskAssignmentService recoveryTaskAssignmentService =
          new RecoveryTaskAssignmentService(
              recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);
      services.add(recoveryTaskAssignmentService);

      ReplicaAssignmentService replicaAssignmentService =
          new ReplicaAssignmentService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
      services.add(replicaAssignmentService);

      SnapshotDeletionService snapshotDeletionService =
          new SnapshotDeletionService(
              replicaMetadataStore, snapshotMetadataStore, blobFs, managerConfig, meterRegistry);
      services.add(snapshotDeletionService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.RECOVERY)) {
      final KaldbConfigs.RecoveryConfig recoveryConfig = kaldbConfig.getRecoveryConfig();
      final int serverPort = recoveryConfig.getServerConfig().getServerPort();

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbRecovery", meterRegistry)
              .withTracing(kaldbConfig.getTracingConfig())
              .build();
      services.add(armeriaService);

      RecoveryService recoveryService =
          new RecoveryService(kaldbConfig, metadataStore, meterRegistry, blobFs);
      services.add(recoveryService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.PREPROCESSOR)) {
      final KaldbConfigs.PreprocessorConfig preprocessorConfig =
          kaldbConfig.getPreprocessorConfig();
      final int serverPort = preprocessorConfig.getServerConfig().getServerPort();

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbPreprocessor", meterRegistry)
              .withTracing(kaldbConfig.getTracingConfig())
              .build();
      services.add(armeriaService);

      ServiceMetadataStore serviceMetadataStore = new ServiceMetadataStore(metadataStore, true);
      PreprocessorService preprocessorService =
          new PreprocessorService(serviceMetadataStore, preprocessorConfig, meterRegistry);
      services.add(preprocessorService);
    }

    return services;
  }

  private static ServiceManager.Listener getServiceManagerListener() {
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

  void shutdown() {
    LOG.info("Running shutdown hook.");
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

  private static void setupSystemMetrics(MeterRegistry prometheusMeterRegistry) {
    // Expose JVM metrics.
    new ClassLoaderMetrics().bindTo(prometheusMeterRegistry);
    new JvmMemoryMetrics().bindTo(prometheusMeterRegistry);
    new JvmGcMetrics().bindTo(prometheusMeterRegistry);
    new ProcessorMetrics().bindTo(prometheusMeterRegistry);
    new JvmThreadMetrics().bindTo(prometheusMeterRegistry);

    LOG.info("Done registering standard JVM metrics for indexer service");
  }
}
