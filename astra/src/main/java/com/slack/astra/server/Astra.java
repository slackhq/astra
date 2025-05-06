package com.slack.astra.server;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3AsyncUtil;
import com.slack.astra.bulkIngestApi.BulkIngestApi;
import com.slack.astra.bulkIngestApi.BulkIngestKafkaProducer;
import com.slack.astra.bulkIngestApi.DatasetRateLimitingService;
import com.slack.astra.chunkManager.CachingChunkManager;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.clusterManager.CacheNodeAssignmentService;
import com.slack.astra.clusterManager.CacheNodeSearchabilityService;
import com.slack.astra.clusterManager.ClusterHpaMetricService;
import com.slack.astra.clusterManager.ClusterMonitorService;
import com.slack.astra.clusterManager.RecoveryTaskAssignmentService;
import com.slack.astra.clusterManager.RedactionUpdateService;
import com.slack.astra.clusterManager.ReplicaAssignmentService;
import com.slack.astra.clusterManager.ReplicaCreationService;
import com.slack.astra.clusterManager.ReplicaDeletionService;
import com.slack.astra.clusterManager.ReplicaEvictionService;
import com.slack.astra.clusterManager.ReplicaRestoreService;
import com.slack.astra.clusterManager.SnapshotDeletionService;
import com.slack.astra.elasticsearchApi.ElasticsearchApiService;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.schema.ReservedFields;
import com.slack.astra.logstore.search.AstraDistributedQueryService;
import com.slack.astra.logstore.search.AstraLocalQueryService;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.CloseableLifecycleManager;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.metadata.hpa.HpaMetricMetadataStore;
import com.slack.astra.metadata.preprocessor.PreprocessorMetadataStore;
import com.slack.astra.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.astra.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.schema.SchemaUtil;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.recovery.RecoveryService;
import com.slack.astra.util.RuntimeHalterImpl;
import com.slack.astra.zipkinApi.ZipkinService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Main class of Astra that sets up the basic infra needed for all the other end points like an a
 * http server, register monitoring libraries, create config manager etc..
 */
public class Astra {
  private static final Logger LOG = LoggerFactory.getLogger(Astra.class);

  private final PrometheusMeterRegistry prometheusMeterRegistry;

  private final AstraConfigs.AstraConfig astraConfig;
  private final S3AsyncClient s3Client;
  protected ServiceManager serviceManager;
  protected AsyncCuratorFramework curatorFramework;

  Astra(
      AstraConfigs.AstraConfig astraConfig,
      S3AsyncClient s3Client,
      PrometheusMeterRegistry prometheusMeterRegistry) {
    this.prometheusMeterRegistry = prometheusMeterRegistry;
    this.astraConfig = astraConfig;
    this.s3Client = s3Client;
    Metrics.addRegistry(prometheusMeterRegistry);
    LOG.info("Started Astra process with config: {}", astraConfig);
  }

  Astra(AstraConfigs.AstraConfig astraConfig, PrometheusMeterRegistry prometheusMeterRegistry) {
    this(astraConfig, S3AsyncUtil.initS3Client(astraConfig.getS3Config()), prometheusMeterRegistry);
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      LOG.info("Config file is needed a first argument");
    }
    Path configFilePath = Path.of(args[0]);

    AstraConfig.initFromFile(configFilePath);
    AstraConfigs.AstraConfig config = AstraConfig.get();
    Astra astra = new Astra(AstraConfig.get(), initPrometheusMeterRegistry(config));
    astra.start();
  }

  static PrometheusMeterRegistry initPrometheusMeterRegistry(AstraConfigs.AstraConfig config) {
    PrometheusMeterRegistry prometheusMeterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    prometheusMeterRegistry
        .config()
        .commonTags(
            "astra_cluster_name",
            config.getClusterConfig().getClusterName(),
            "astra_env",
            config.getClusterConfig().getEnv(),
            "astra_component",
            getComponentTag(config));
    return prometheusMeterRegistry;
  }

  private static String getComponentTag(AstraConfigs.AstraConfig config) {
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

    curatorFramework =
        CuratorBuilder.build(
            prometheusMeterRegistry, astraConfig.getMetadataStoreConfig().getZookeeperConfig());
    BlobStore blobStore = new BlobStore(s3Client, astraConfig.getS3Config().getS3Bucket());

    Set<Service> services =
        getServices(curatorFramework, astraConfig, blobStore, prometheusMeterRegistry);
    serviceManager = new ServiceManager(services);
    serviceManager.addListener(getServiceManagerListener(), MoreExecutors.directExecutor());

    serviceManager.startAsync();
  }

  private static Set<Service> getServices(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.AstraConfig astraConfig,
      BlobStore blobStore,
      PrometheusMeterRegistry meterRegistry)
      throws Exception {
    Set<Service> services = new HashSet<>();

    HashSet<AstraConfigs.NodeRole> roles = new HashSet<>(astraConfig.getNodeRolesList());

    if (roles.contains(AstraConfigs.NodeRole.INDEX)) {
      IndexingChunkManager<LogMessage> chunkManager =
          IndexingChunkManager.fromConfig(
              meterRegistry,
              curatorFramework,
              astraConfig.getIndexerConfig(),
              astraConfig.getMetadataStoreConfig(),
              blobStore,
              astraConfig.getS3Config());
      services.add(chunkManager);

      AstraIndexer indexer =
          new AstraIndexer(
              chunkManager,
              curatorFramework,
              astraConfig.getMetadataStoreConfig(),
              astraConfig.getIndexerConfig(),
              astraConfig.getIndexerConfig().getKafkaConfig(),
              meterRegistry);
      services.add(indexer);

      AstraLocalQueryService<LogMessage> searcher =
          new AstraLocalQueryService<>(
              chunkManager,
              Duration.ofMillis(astraConfig.getIndexerConfig().getDefaultQueryTimeoutMs()));
      final int serverPort = astraConfig.getIndexerConfig().getServerConfig().getServerPort();
      Duration requestTimeout =
          Duration.ofMillis(astraConfig.getIndexerConfig().getServerConfig().getRequestTimeoutMs());

      FieldRedactionMetadataStore fieldRedactionMetadataStore =
          new FieldRedactionMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);
      RedactionUpdateService redactionUpdateService =
          new RedactionUpdateService(
              fieldRedactionMetadataStore, astraConfig.getRedactionUpdateServiceConfig());
      services.add(redactionUpdateService);

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "astraIndex", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(astraConfig.getTracingConfig())
              .withGrpcService(searcher)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(AstraConfigs.NodeRole.QUERY)) {
      SearchMetadataStore searchMetadataStore =
          new SearchMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);
      SnapshotMetadataStore snapshotMetadataStore =
          new SnapshotMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry);
      DatasetMetadataStore datasetMetadataStore =
          new DatasetMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);

      services.add(
          new CloseableLifecycleManager(
              AstraConfigs.NodeRole.QUERY,
              List.of(searchMetadataStore, snapshotMetadataStore, datasetMetadataStore)));

      Duration requestTimeout =
          Duration.ofMillis(astraConfig.getQueryConfig().getServerConfig().getRequestTimeoutMs());
      AstraDistributedQueryService astraDistributedQueryService =
          new AstraDistributedQueryService(
              searchMetadataStore,
              snapshotMetadataStore,
              datasetMetadataStore,
              meterRegistry,
              requestTimeout,
              Duration.ofMillis(astraConfig.getQueryConfig().getDefaultQueryTimeoutMs()));
      // todo - close the astraDistributedQueryService once done (depends on
      // https://github.com/slackhq/astra/pull/564)
      final int serverPort = astraConfig.getQueryConfig().getServerConfig().getServerPort();

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "astraQuery", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(astraConfig.getTracingConfig())
              .withAnnotatedService(new ElasticsearchApiService(astraDistributedQueryService))
              .withAnnotatedService(
                  new ZipkinService(
                      astraDistributedQueryService,
                      astraConfig.getQueryConfig().getZipkinDefaultMaxSpans(),
                      astraConfig.getQueryConfig().getZipkinDefaultLookbackMins()))
              .withGrpcService(astraDistributedQueryService)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(AstraConfigs.NodeRole.CACHE)) {
      CachingChunkManager<LogMessage> chunkManager =
          CachingChunkManager.fromConfig(
              meterRegistry,
              curatorFramework,
              astraConfig.getMetadataStoreConfig(),
              astraConfig.getS3Config(),
              astraConfig.getCacheConfig(),
              blobStore);
      services.add(chunkManager);

      HpaMetricMetadataStore hpaMetricMetadataStore =
          new HpaMetricMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);
      FieldRedactionMetadataStore fieldRedactionMetadataStore =
          new FieldRedactionMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);

      services.add(
          new CloseableLifecycleManager(
              AstraConfigs.NodeRole.CACHE, List.of(hpaMetricMetadataStore)));
      HpaMetricPublisherService hpaMetricPublisherService =
          new HpaMetricPublisherService(
              hpaMetricMetadataStore, meterRegistry, Metadata.HpaMetricMetadata.NodeRole.CACHE);
      services.add(hpaMetricPublisherService);
      RedactionUpdateService redactionUpdateService =
          new RedactionUpdateService(
              fieldRedactionMetadataStore, astraConfig.getRedactionUpdateServiceConfig());
      services.add(redactionUpdateService);

      AstraLocalQueryService<LogMessage> searcher =
          new AstraLocalQueryService<>(
              chunkManager,
              Duration.ofMillis(astraConfig.getCacheConfig().getDefaultQueryTimeoutMs()));
      final int serverPort = astraConfig.getCacheConfig().getServerConfig().getServerPort();
      Duration requestTimeout =
          Duration.ofMillis(astraConfig.getCacheConfig().getServerConfig().getRequestTimeoutMs());
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "astraCache", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(astraConfig.getTracingConfig())
              .withGrpcService(searcher)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(AstraConfigs.NodeRole.MANAGER)) {
      final AstraConfigs.ManagerConfig managerConfig = astraConfig.getManagerConfig();
      final int serverPort = managerConfig.getServerConfig().getServerPort();

      ReplicaMetadataStore replicaMetadataStore =
          new ReplicaMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry);
      SnapshotMetadataStore snapshotMetadataStore =
          new SnapshotMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry);
      RecoveryTaskMetadataStore recoveryTaskMetadataStore =
          new RecoveryTaskMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);
      RecoveryNodeMetadataStore recoveryNodeMetadataStore =
          new RecoveryNodeMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);
      CacheSlotMetadataStore cacheSlotMetadataStore =
          new CacheSlotMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry);
      DatasetMetadataStore datasetMetadataStore =
          new DatasetMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);
      HpaMetricMetadataStore hpaMetricMetadataStore =
          new HpaMetricMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);
      FieldRedactionMetadataStore fieldRedactionMetadataStore =
          new FieldRedactionMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);

      Duration requestTimeout =
          Duration.ofMillis(astraConfig.getManagerConfig().getServerConfig().getRequestTimeoutMs());
      ReplicaRestoreService replicaRestoreService =
          new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);
      services.add(replicaRestoreService);

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "astraManager", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(astraConfig.getTracingConfig())
              .withGrpcService(
                  new ManagerApiGrpc(
                      datasetMetadataStore,
                      snapshotMetadataStore,
                      replicaRestoreService,
                      fieldRedactionMetadataStore))
              .build();
      services.add(armeriaService);

      services.add(
          new CloseableLifecycleManager(
              AstraConfigs.NodeRole.MANAGER,
              List.of(
                  replicaMetadataStore,
                  snapshotMetadataStore,
                  recoveryTaskMetadataStore,
                  recoveryNodeMetadataStore,
                  cacheSlotMetadataStore,
                  datasetMetadataStore,
                  hpaMetricMetadataStore)));

      ReplicaCreationService replicaCreationService =
          new ReplicaCreationService(
              replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
      services.add(replicaCreationService);

      ReplicaEvictionService replicaEvictionService =
          new ReplicaEvictionService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
      services.add(replicaEvictionService);

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
              replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);
      services.add(snapshotDeletionService);

      CacheNodeMetadataStore cacheNodeMetadataStore =
          new CacheNodeMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry);
      CacheNodeAssignmentStore cacheNodeAssignmentStore =
          new CacheNodeAssignmentStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry);

      ClusterHpaMetricService clusterHpaMetricService =
          new ClusterHpaMetricService(
              replicaMetadataStore,
              cacheSlotMetadataStore,
              hpaMetricMetadataStore,
              cacheNodeMetadataStore,
              snapshotMetadataStore);
      services.add(clusterHpaMetricService);

      ClusterMonitorService clusterMonitorService =
          new ClusterMonitorService(
              replicaMetadataStore,
              snapshotMetadataStore,
              recoveryTaskMetadataStore,
              recoveryNodeMetadataStore,
              cacheSlotMetadataStore,
              datasetMetadataStore,
              cacheNodeAssignmentStore,
              cacheNodeMetadataStore,
              managerConfig,
              meterRegistry);
      services.add(clusterMonitorService);

      ReplicaDeletionService replicaDeletionService =
          new ReplicaDeletionService(
              cacheSlotMetadataStore,
              replicaMetadataStore,
              cacheNodeAssignmentStore,
              managerConfig,
              meterRegistry);
      services.add(replicaDeletionService);

      CacheNodeAssignmentService cacheNodeAssignmentService =
          new CacheNodeAssignmentService(
              meterRegistry,
              managerConfig,
              replicaMetadataStore,
              cacheNodeMetadataStore,
              snapshotMetadataStore,
              cacheNodeAssignmentStore);
      services.add(cacheNodeAssignmentService);

      RedactionUpdateService redactionUpdateService =
          new RedactionUpdateService(
              fieldRedactionMetadataStore, astraConfig.getRedactionUpdateServiceConfig());
      services.add(redactionUpdateService);

      SearchMetadataStore searchMetadataStore =
          new SearchMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);
      CacheNodeSearchabilityService cacheNodeSearchabilityService =
          new CacheNodeSearchabilityService(
              meterRegistry,
              cacheNodeMetadataStore,
              managerConfig,
              cacheNodeAssignmentStore,
              searchMetadataStore,
              snapshotMetadataStore);
      services.add(cacheNodeSearchabilityService);
    }

    if (roles.contains(AstraConfigs.NodeRole.RECOVERY)) {
      final AstraConfigs.RecoveryConfig recoveryConfig = astraConfig.getRecoveryConfig();
      final int serverPort = recoveryConfig.getServerConfig().getServerPort();

      Duration requestTimeout =
          Duration.ofMillis(
              astraConfig.getRecoveryConfig().getServerConfig().getRequestTimeoutMs());
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "astraRecovery", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(astraConfig.getTracingConfig())
              .build();
      services.add(armeriaService);

      RecoveryService recoveryService =
          new RecoveryService(astraConfig, curatorFramework, meterRegistry, blobStore);
      services.add(recoveryService);
    }

    if (roles.contains(AstraConfigs.NodeRole.PREPROCESSOR)) {
      DatasetMetadataStore datasetMetadataStore =
          new DatasetMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);

      PreprocessorMetadataStore preprocessorMetadataStore =
          new PreprocessorMetadataStore(
              curatorFramework, astraConfig.getMetadataStoreConfig(), meterRegistry, true);

      final AstraConfigs.PreprocessorConfig preprocessorConfig =
          astraConfig.getPreprocessorConfig();
      final int serverPort = preprocessorConfig.getServerConfig().getServerPort();

      Duration requestTimeout =
          Duration.ofMillis(
              astraConfig.getPreprocessorConfig().getServerConfig().getRequestTimeoutMs());
      ArmeriaService.Builder armeriaServiceBuilder =
          new ArmeriaService.Builder(serverPort, "astraPreprocessor", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(astraConfig.getTracingConfig());

      services.add(
          new CloseableLifecycleManager(
              AstraConfigs.NodeRole.PREPROCESSOR,
              List.of(datasetMetadataStore, preprocessorMetadataStore)));

      BulkIngestKafkaProducer bulkIngestKafkaProducer =
          new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);
      services.add(bulkIngestKafkaProducer);

      DatasetRateLimitingService datasetRateLimitingService =
          new DatasetRateLimitingService(
              datasetMetadataStore, preprocessorMetadataStore, preprocessorConfig, meterRegistry);
      services.add(datasetRateLimitingService);

      Schema.IngestSchema schema = Schema.IngestSchema.getDefaultInstance();
      if (!preprocessorConfig.getSchemaFile().isEmpty()) {
        LOG.info("Loading schema file: {}", preprocessorConfig.getSchemaFile());
        schema = SchemaUtil.parseSchema(Path.of(preprocessorConfig.getSchemaFile()));
        LOG.info(
            "Loaded schema with fields count: {}, defaults count: {}",
            schema.getFieldsCount(),
            schema.getDefaultsCount());
      } else {
        LOG.info("No schema file provided, using default schema");
      }
      schema = ReservedFields.addPredefinedFields(schema);
      BulkIngestApi openSearchBulkApiService =
          new BulkIngestApi(
              bulkIngestKafkaProducer,
              datasetRateLimitingService,
              meterRegistry,
              preprocessorConfig.getRateLimitExceededErrorCode(),
              schema);
      armeriaServiceBuilder.withAnnotatedService(openSearchBulkApiService);
      services.add(armeriaServiceBuilder.build());
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
            .handleFatal(new Throwable("Shutting down Astra due to failed service"));
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
      curatorFramework.unwrap().close();
    } catch (Exception e) {
      LOG.error("Error while closing curatorFramework ", e);
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
