package com.slack.astra.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.config.AstraConfigs;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AstraConfigTest {

  @BeforeEach
  public void setUp() {
    AstraConfig.reset();
  }

  @AfterEach
  public void tearDown() {
    AstraConfig.reset();
  }

  @Test
  public void testInitWithMissingConfigFile() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> AstraConfig.initFromFile(Path.of("missing_config_file.json")));
  }

  @Test
  public void testEmptyJsonCfgFile() {
    assertThatExceptionOfType(InvalidProtocolBufferException.class)
        .isThrownBy(() -> AstraConfig.fromJsonConfig(""));
  }

  @Test
  public void testIntToStrTypeConversionForWrongJsonType()
      throws InvalidProtocolBufferException, JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode serverConfig = mapper.createObjectNode().put("requestTimeoutMs", 3000);
    ObjectNode indexerConfig =
        mapper
            .createObjectNode()
            .put("maxMessagesPerChunk", 1)
            .put("maxBytesPerChunk", 100)
            .put("defaultQueryTimeoutMs", "2500")
            .set("serverConfig", serverConfig);
    ObjectNode kafkaConfig =
        mapper.createObjectNode().put("kafkaTopicPartition", 1).put("kafkaSessionTimeout", 30000);
    indexerConfig.set("kafkaConfig", kafkaConfig);

    ObjectNode node = mapper.createObjectNode();
    node.set("nodeRoles", mapper.createArrayNode().add("INDEX"));
    node.set("indexerConfig", indexerConfig);
    final String missingRequiredField =
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);

    final AstraConfigs.AstraConfig astraConfig = AstraConfig.fromJsonConfig(missingRequiredField);

    final AstraConfigs.KafkaConfig kafkaCfg = astraConfig.getIndexerConfig().getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("1");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");
  }

  @Test
  public void testStrToIntTypeConversionForWrongJsonType()
      throws InvalidProtocolBufferException, JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode serverConfig = mapper.createObjectNode().put("requestTimeoutMs", 3000);
    ObjectNode indexerConfig =
        mapper
            .createObjectNode()
            .put("maxMessagesPerChunk", 1)
            .put("maxBytesPerChunk", 100)
            .put("defaultQueryTimeoutMs", "2500")
            .set("serverConfig", serverConfig);
    ObjectNode node = mapper.createObjectNode();
    node.set("nodeRoles", mapper.createArrayNode().add("INDEX"));
    node.set("indexerConfig", indexerConfig);
    final String missingRequiredField =
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);

    final AstraConfigs.AstraConfig astraConfig = AstraConfig.fromJsonConfig(missingRequiredField);

    final AstraConfigs.IndexerConfig indexerCfg = astraConfig.getIndexerConfig();
    assertThat(indexerCfg.getMaxMessagesPerChunk()).isEqualTo(1);
    assertThat(indexerCfg.getMaxBytesPerChunk()).isEqualTo(100);
  }

  @Test
  public void testIgnoreExtraConfigField() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode serverConfig = mapper.createObjectNode().put("requestTimeoutMs", 3000);
    ObjectNode kafkaConfig =
        mapper
            .createObjectNode()
            .put("kafkaTopicPartition", 1)
            .put("kafkaSessionTimeout", 30000)
            .put("ignoreExtraField", "ignoredField");
    ObjectNode indexerConfig =
        mapper
            .createObjectNode()
            .put("maxMessagesPerChunk", 1)
            .put("maxBytesPerChunk", 100)
            .put("ignoredField", "ignore")
            .put("defaultQueryTimeoutMs", "2500")
            .set("serverConfig", serverConfig);
    indexerConfig.set("kafkaConfig", kafkaConfig);

    ObjectNode node = mapper.createObjectNode();
    node.set("nodeRoles", mapper.createArrayNode().add("INDEX"));
    node.set("indexerConfig", indexerConfig);

    final String configWithExtraField =
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);

    final AstraConfigs.AstraConfig astraConfig = AstraConfig.fromJsonConfig(configWithExtraField);

    final AstraConfigs.KafkaConfig kafkaCfg = astraConfig.getIndexerConfig().getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("1");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");
    assertThat(kafkaCfg.getKafkaTopic()).isEmpty();

    final AstraConfigs.IndexerConfig indexerCfg = astraConfig.getIndexerConfig();
    assertThat(indexerCfg.getMaxMessagesPerChunk()).isEqualTo(1);
    assertThat(indexerCfg.getMaxBytesPerChunk()).isEqualTo(100);
    assertThat(astraConfig.getNodeRolesList()).containsExactly(AstraConfigs.NodeRole.INDEX);

    final AstraConfigs.S3Config s3Config = astraConfig.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEmpty();
    assertThat(s3Config.getS3SecretKey()).isEmpty();
    assertThat(s3Config.getS3Region()).isEmpty();
    assertThat(s3Config.getS3EndPoint()).isEmpty();
  }

  @Test
  public void testParseAstraJsonConfigFile() throws IOException {
    final File cfgFile =
        new File(getClass().getClassLoader().getResource("test_config.json").getFile());
    AstraConfig.initFromFile(cfgFile.toPath());
    final AstraConfigs.AstraConfig config = AstraConfig.get();

    assertThat(config).isNotNull();

    assertThat(config.getNodeRolesList().size()).isEqualTo(4);
    assertThat(config.getNodeRolesList().get(0)).isEqualTo(AstraConfigs.NodeRole.INDEX);
    assertThat(config.getNodeRolesList().get(1)).isEqualTo(AstraConfigs.NodeRole.QUERY);
    assertThat(config.getNodeRolesList().get(2)).isEqualTo(AstraConfigs.NodeRole.CACHE);
    assertThat(config.getNodeRolesList().get(3)).isEqualTo(AstraConfigs.NodeRole.MANAGER);

    final AstraConfigs.KafkaConfig kafkaCfg = config.getIndexerConfig().getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopic()).isEqualTo("testTopic");
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("1");
    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEqualTo("kafka.us-east-1.consul:9094");
    assertThat(kafkaCfg.getKafkaClientGroup()).isEqualTo("astra-test");
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEqualTo("true");
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEqualTo("5000");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");

    assertThat(kafkaCfg.getAdditionalProps().size()).isGreaterThan(0);
    assertThat(kafkaCfg.getAdditionalPropsMap().get("fetch.max.bytes")).isEqualTo("100");

    final AstraConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEqualTo("access");
    assertThat(s3Config.getS3SecretKey()).isEqualTo("secret");
    assertThat(s3Config.getS3Region()).isEqualTo("us-east-1");
    assertThat(s3Config.getS3EndPoint()).isEqualTo("https://s3.us-east-1.amazonaws.com/");
    assertThat(s3Config.getS3Bucket()).isEqualTo("test-s3-bucket");

    final AstraConfigs.TracingConfig tracingConfig = config.getTracingConfig();
    assertThat(tracingConfig.getZipkinEndpoint()).isEqualTo("http://localhost:9411/api/v2/spans");
    assertThat(tracingConfig.getCommonTagsMap()).isEqualTo(Map.of("clusterName", "astra_local"));
    assertThat(tracingConfig.getSamplingRate()).isEqualTo(1.0f);

    final AstraConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isEqualTo(1000);
    assertThat(indexerConfig.getMaxBytesPerChunk()).isEqualTo(100000);
    assertThat(indexerConfig.getLuceneConfig().getCommitDurationSecs()).isEqualTo(10);
    assertThat(indexerConfig.getLuceneConfig().getRefreshDurationSecs()).isEqualTo(11);
    assertThat(indexerConfig.getLuceneConfig().getEnableFullTextSearch()).isTrue();
    assertThat(indexerConfig.getMaxChunksOnDisk()).isEqualTo(3);
    assertThat(indexerConfig.getStaleDurationSecs()).isEqualTo(7200);
    assertThat(indexerConfig.getDataDirectory()).isEqualTo("/tmp");
    assertThat(indexerConfig.getServerConfig().getServerPort()).isEqualTo(8080);
    assertThat(indexerConfig.getServerConfig().getServerAddress()).isEqualTo("localhost");
    assertThat(indexerConfig.getServerConfig().getRequestTimeoutMs()).isEqualTo(3000);
    assertThat(indexerConfig.getMaxOffsetDelayMessages()).isEqualTo(10002);

    final AstraConfigs.QueryServiceConfig queryServiceConfig = config.getQueryConfig();
    assertThat(queryServiceConfig.getServerConfig().getServerPort()).isEqualTo(8081);
    assertThat(queryServiceConfig.getServerConfig().getServerAddress()).isEqualTo("1.2.3.4");
    assertThat(queryServiceConfig.getManagerConnectString()).isEqualTo("localhost:8083");
    assertThat(queryServiceConfig.getZipkinDefaultLookbackMins())
        .isEqualTo(10080); // 7 days in minutes

    final AstraConfigs.MetadataStoreConfig metadataStoreConfig = config.getMetadataStoreConfig();
    final AstraConfigs.ZookeeperConfig zookeeperConfig = metadataStoreConfig.getZookeeperConfig();
    assertThat(zookeeperConfig.getZkConnectString()).isEqualTo("1.2.3.4:9092");
    assertThat(zookeeperConfig.getZkPathPrefix()).isEqualTo("zkPrefix");
    assertThat(zookeeperConfig.getZkSessionTimeoutMs()).isEqualTo(1000);
    assertThat(zookeeperConfig.getZkConnectionTimeoutMs()).isEqualTo(1500);
    assertThat(zookeeperConfig.getSleepBetweenRetriesMs()).isEqualTo(500);
    assertThat(metadataStoreConfig.getMode())
        .isEqualTo(AstraConfigs.MetadataStoreMode.BOTH_READ_ZOOKEEPER_WRITE);

    final AstraConfigs.CacheConfig cacheConfig = config.getCacheConfig();
    final AstraConfigs.ServerConfig cacheServerConfig = cacheConfig.getServerConfig();
    assertThat(cacheConfig.getSlotsPerInstance()).isEqualTo(10);
    assertThat(cacheConfig.getDataDirectory()).isEqualTo("/tmp");
    assertThat(cacheServerConfig.getServerPort()).isEqualTo(8082);
    assertThat(cacheServerConfig.getServerAddress()).isEqualTo("localhost");

    final AstraConfigs.ManagerConfig managerConfig = config.getManagerConfig();
    assertThat(managerConfig.getEventAggregationSecs()).isEqualTo(10);
    assertThat(managerConfig.getScheduleInitialDelayMins()).isEqualTo(1);

    final AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        managerConfig.getReplicaCreationServiceConfig();
    assertThat(replicaCreationServiceConfig.getReplicaSetsList()).isEqualTo(List.of("rep1"));
    assertThat(replicaCreationServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(replicaCreationServiceConfig.getReplicaLifespanMins()).isEqualTo(1440);

    final AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        managerConfig.getReplicaEvictionServiceConfig();
    assertThat(replicaEvictionServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        managerConfig.getReplicaDeletionServiceConfig();
    assertThat(replicaDeletionServiceConfig.getSchedulePeriodMins()).isEqualTo(90);

    final AstraConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            managerConfig.getRecoveryTaskAssignmentServiceConfig();
    assertThat(recoveryTaskAssignmentServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        managerConfig.getReplicaAssignmentServiceConfig();
    assertThat(replicaAssignmentServiceConfig.getSchedulePeriodMins()).isEqualTo(10);
    assertThat(replicaAssignmentServiceConfig.getReplicaSetsList()).containsExactly("rep1");
    assertThat(replicaAssignmentServiceConfig.getMaxConcurrentPerNode()).isEqualTo(2);

    final AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        managerConfig.getSnapshotDeletionServiceConfig();
    assertThat(snapshotDeletionServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(snapshotDeletionServiceConfig.getSnapshotLifespanMins()).isEqualTo(10080);

    final AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRestoreServiceConfig =
        managerConfig.getReplicaRestoreServiceConfig();
    assertThat(replicaRestoreServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(replicaRestoreServiceConfig.getMaxReplicasPerRequest()).isEqualTo(200);
    assertThat(replicaRestoreServiceConfig.getReplicaLifespanMins()).isEqualTo(60);

    final AstraConfigs.ServerConfig managerServerConfig = managerConfig.getServerConfig();
    assertThat(managerServerConfig.getServerPort()).isEqualTo(8083);
    assertThat(managerServerConfig.getServerAddress()).isEqualTo("localhost");

    final AstraConfigs.ClusterConfig clusterConfig = config.getClusterConfig();
    assertThat(clusterConfig.getClusterName()).isEqualTo("test_astra_json_cluster");
    assertThat(clusterConfig.getEnv()).isEqualTo("env_test");

    final AstraConfigs.RecoveryConfig recoveryConfig = config.getRecoveryConfig();
    final AstraConfigs.ServerConfig recoveryServerConfig = recoveryConfig.getServerConfig();
    assertThat(recoveryServerConfig.getServerPort()).isEqualTo(8084);
    assertThat(recoveryServerConfig.getServerAddress()).isEqualTo("localhost");

    final AstraConfigs.PreprocessorConfig preprocessorConfig = config.getPreprocessorConfig();
    assertThat(preprocessorConfig.getPreprocessorInstanceCount()).isEqualTo(1);
    assertThat(preprocessorConfig.getRateLimiterMaxBurstSeconds()).isEqualTo(2);
    assertThat(preprocessorConfig.getRateLimitExceededErrorCode()).isEqualTo(400);
    assertThat(preprocessorConfig.getSchemaFile()).isEqualTo("schema/test_schema.yaml");

    final AstraConfigs.KafkaConfig preprocessorKafkaConfig =
        config.getPreprocessorConfig().getKafkaConfig();
    assertThat(preprocessorKafkaConfig.getKafkaBootStrapServers()).isEqualTo("localhost:9092");
    assertThat(preprocessorKafkaConfig.getKafkaTopic()).isEqualTo("test-topic");

    final AstraConfigs.ServerConfig preprocessorServerConfig = preprocessorConfig.getServerConfig();
    assertThat(preprocessorServerConfig.getServerPort()).isEqualTo(8085);
    assertThat(preprocessorServerConfig.getServerAddress()).isEqualTo("localhost");
  }

  @Test
  public void testMapSubstitution() throws IOException {
    final File cfgFile =
        new File(getClass().getClassLoader().getResource("test_config.yaml").getFile());

    // space is crucial after the key:
    System.setProperty("KAFKA_ADDITIONAL_PROPS", "{fetch.max.bytes: 100,linger.ms: 123}");
    AstraConfig.initFromFile(cfgFile.toPath());
    final AstraConfigs.AstraConfig config =
        AstraConfig.fromYamlConfig(Files.readString(cfgFile.toPath()), System::getProperty);

    assertThat(config).isNotNull();

    final AstraConfigs.KafkaConfig kafkaCfg = config.getIndexerConfig().getKafkaConfig();
    assertThat(kafkaCfg.getAdditionalPropsMap().size()).isEqualTo(2);
    assertThat(kafkaCfg.getAdditionalPropsMap().get("fetch.max.bytes")).isEqualTo("100");
    assertThat(kafkaCfg.getAdditionalPropsMap().get("linger.ms")).isEqualTo("123");
  }

  @Test
  public void testParseAstraYamlConfigFile() throws IOException {
    final File cfgFile =
        new File(getClass().getClassLoader().getResource("test_config.yaml").getFile());

    AstraConfig.initFromFile(cfgFile.toPath());
    final AstraConfigs.AstraConfig config = AstraConfig.get();

    assertThat(config).isNotNull();

    assertThat(config.getNodeRolesList().size()).isEqualTo(4);
    assertThat(config.getNodeRolesList().get(0)).isEqualTo(AstraConfigs.NodeRole.INDEX);
    assertThat(config.getNodeRolesList().get(1)).isEqualTo(AstraConfigs.NodeRole.QUERY);
    assertThat(config.getNodeRolesList().get(2)).isEqualTo(AstraConfigs.NodeRole.CACHE);
    assertThat(config.getNodeRolesList().get(3)).isEqualTo(AstraConfigs.NodeRole.MANAGER);

    final AstraConfigs.KafkaConfig kafkaCfg = config.getIndexerConfig().getKafkaConfig();

    // todo - for testing env var substitution we could use something like Mockito (or similar) in
    // the future
    assertThat(kafkaCfg.getKafkaTopic()).isEqualTo("test-topic");

    // uses default fallback as we expect the env var NOT_PRESENT to not be set
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("0");

    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEqualTo("localhost:9092");
    assertThat(kafkaCfg.getKafkaClientGroup()).isEqualTo("astra-test");
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEqualTo("true");
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEqualTo("5000");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");

    assertThat(kafkaCfg.getAdditionalPropsMap().size()).isEqualTo(0);

    final AstraConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEqualTo("access");
    assertThat(s3Config.getS3SecretKey()).isEqualTo("secret");
    assertThat(s3Config.getS3Region()).isEqualTo("us-east-1");
    assertThat(s3Config.getS3EndPoint()).isEqualTo("localhost:9090");
    assertThat(s3Config.getS3Bucket()).isEqualTo("test-s3-bucket");

    final AstraConfigs.TracingConfig tracingConfig = config.getTracingConfig();
    assertThat(tracingConfig.getZipkinEndpoint()).isEqualTo("http://localhost:9411/api/v2/spans");
    assertThat(tracingConfig.getCommonTagsMap()).isEqualTo(Map.of("clusterName", "astra_local"));

    final AstraConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isEqualTo(100);
    assertThat(indexerConfig.getMaxBytesPerChunk()).isEqualTo(100000);
    assertThat(indexerConfig.getMaxTimePerChunkSeconds()).isEqualTo(1800);
    assertThat(indexerConfig.getLuceneConfig().getCommitDurationSecs()).isEqualTo(10);
    assertThat(indexerConfig.getLuceneConfig().getRefreshDurationSecs()).isEqualTo(11);
    assertThat(indexerConfig.getLuceneConfig().getEnableFullTextSearch()).isTrue();
    assertThat(indexerConfig.getMaxChunksOnDisk()).isEqualTo(3);
    assertThat(indexerConfig.getStaleDurationSecs()).isEqualTo(7200);
    assertThat(indexerConfig.getDataDirectory()).isEqualTo("/tmp");
    assertThat(indexerConfig.getMaxOffsetDelayMessages()).isEqualTo(10001);
    assertThat(indexerConfig.getServerConfig().getServerPort()).isEqualTo(8080);
    assertThat(indexerConfig.getServerConfig().getServerAddress()).isEqualTo("localhost");

    final AstraConfigs.QueryServiceConfig readConfig = config.getQueryConfig();
    assertThat(readConfig.getServerConfig().getServerPort()).isEqualTo(8081);
    assertThat(readConfig.getServerConfig().getServerAddress()).isEqualTo("1.2.3.4");
    assertThat(readConfig.getManagerConnectString()).isEqualTo("localhost:8083");
    assertThat(readConfig.getZipkinDefaultLookbackMins()).isEqualTo(10080); // 7 days in minutes

    final AstraConfigs.MetadataStoreConfig metadataStoreConfig = config.getMetadataStoreConfig();
    final AstraConfigs.ZookeeperConfig zookeeperConfig = metadataStoreConfig.getZookeeperConfig();
    assertThat(zookeeperConfig.getZkConnectString()).isEqualTo("1.2.3.4:9092");
    assertThat(zookeeperConfig.getZkPathPrefix()).isEqualTo("zkPrefix");
    assertThat(zookeeperConfig.getZkSessionTimeoutMs()).isEqualTo(1000);
    assertThat(zookeeperConfig.getZkConnectionTimeoutMs()).isEqualTo(1500);
    assertThat(zookeeperConfig.getSleepBetweenRetriesMs()).isEqualTo(500);
    assertThat(metadataStoreConfig.getMode())
        .isEqualTo(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE);

    final AstraConfigs.CacheConfig cacheConfig = config.getCacheConfig();
    final AstraConfigs.ServerConfig cacheServerConfig = cacheConfig.getServerConfig();
    assertThat(cacheConfig.getSlotsPerInstance()).isEqualTo(10);
    assertThat(cacheServerConfig.getServerPort()).isEqualTo(8082);
    assertThat(cacheConfig.getDataDirectory()).isEqualTo("/tmp");
    assertThat(cacheServerConfig.getServerAddress()).isEqualTo("localhost");

    final AstraConfigs.ManagerConfig managerConfig = config.getManagerConfig();
    assertThat(managerConfig.getEventAggregationSecs()).isEqualTo(10);
    assertThat(managerConfig.getScheduleInitialDelayMins()).isEqualTo(1);

    final AstraConfigs.ClusterConfig clusterConfig = config.getClusterConfig();
    assertThat(clusterConfig.getClusterName()).isEqualTo("test_astra_cluster");
    assertThat(clusterConfig.getEnv()).isEqualTo("test_astra_env");

    final AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        managerConfig.getReplicaCreationServiceConfig();
    assertThat(replicaCreationServiceConfig.getReplicaSetsList()).isEqualTo(List.of("rep1"));
    assertThat(replicaCreationServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(replicaCreationServiceConfig.getReplicaLifespanMins()).isEqualTo(1440);

    final AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        managerConfig.getReplicaEvictionServiceConfig();
    assertThat(replicaEvictionServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        managerConfig.getReplicaDeletionServiceConfig();
    assertThat(replicaDeletionServiceConfig.getSchedulePeriodMins()).isEqualTo(90);

    final AstraConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            managerConfig.getRecoveryTaskAssignmentServiceConfig();
    assertThat(recoveryTaskAssignmentServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        managerConfig.getReplicaAssignmentServiceConfig();
    assertThat(replicaAssignmentServiceConfig.getSchedulePeriodMins()).isEqualTo(10);
    assertThat(replicaAssignmentServiceConfig.getReplicaSetsList()).containsExactly("rep1");
    assertThat(replicaAssignmentServiceConfig.getMaxConcurrentPerNode()).isEqualTo(2);

    final AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRestoreServiceConfig =
        managerConfig.getReplicaRestoreServiceConfig();
    assertThat(replicaRestoreServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(replicaRestoreServiceConfig.getMaxReplicasPerRequest()).isEqualTo(200);
    assertThat(replicaRestoreServiceConfig.getReplicaLifespanMins()).isEqualTo(60);

    final AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        managerConfig.getSnapshotDeletionServiceConfig();
    assertThat(snapshotDeletionServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(snapshotDeletionServiceConfig.getSnapshotLifespanMins()).isEqualTo(10080);

    final AstraConfigs.ServerConfig managerServerConfig = managerConfig.getServerConfig();
    assertThat(managerServerConfig.getServerPort()).isEqualTo(8083);
    assertThat(managerServerConfig.getServerAddress()).isEqualTo("localhost");

    final AstraConfigs.RecoveryConfig recoveryConfig = config.getRecoveryConfig();
    final AstraConfigs.ServerConfig recoveryServerConfig = recoveryConfig.getServerConfig();
    assertThat(recoveryServerConfig.getServerPort()).isEqualTo(8084);
    assertThat(recoveryServerConfig.getServerAddress()).isEqualTo("localhost");

    final AstraConfigs.PreprocessorConfig preprocessorConfig = config.getPreprocessorConfig();
    assertThat(preprocessorConfig.getPreprocessorInstanceCount()).isEqualTo(1);
    assertThat(preprocessorConfig.getRateLimiterMaxBurstSeconds()).isEqualTo(2);

    final AstraConfigs.KafkaConfig preprocessorKafkaConfig =
        config.getPreprocessorConfig().getKafkaConfig();
    assertThat(preprocessorKafkaConfig.getKafkaBootStrapServers()).isEqualTo("localhost:9092");
    assertThat(preprocessorKafkaConfig.getKafkaTopic()).isEqualTo("test-topic");

    assertThat(preprocessorConfig.getRateLimitExceededErrorCode()).isEqualTo(429);
    assertThat(preprocessorConfig.getSchemaFile()).isEqualTo("schema/test_schema.yaml");

    final AstraConfigs.ServerConfig preprocessorServerConfig = preprocessorConfig.getServerConfig();
    assertThat(preprocessorServerConfig.getServerPort()).isEqualTo(8085);
    assertThat(preprocessorServerConfig.getServerAddress()).isEqualTo("localhost");
  }

  @Test
  public void testParseFormats() {
    // only json/yaml file extentions are supported
    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> AstraConfig.initFromFile(Path.of("README.md")));
  }

  @Test
  public void testMalformedYaml() {
    assertThatExceptionOfType(InvalidProtocolBufferException.class)
        .isThrownBy(() -> AstraConfig.fromYamlConfig(":test"));
  }

  @Test
  public void testMissingDataTransformerConfigForCache() throws InvalidProtocolBufferException {
    AstraConfigs.AstraConfig config =
        AstraConfig.fromJsonConfig(
            "{nodeRoles: [CACHE], "
                + "cacheConfig:{defaultQueryTimeoutMs:2500,serverConfig:{requestTimeoutMs:3000}}}");

    assertThat(config.getNodeRolesList().size()).isEqualTo(1);
    assertThat(config.getNodeRolesList()).containsOnly(AstraConfigs.NodeRole.CACHE);
    assertThat(config.getCacheConfig().getServerConfig().getRequestTimeoutMs()).isEqualTo(3000);
    assertThat(config.getCacheConfig().getDefaultQueryTimeoutMs()).isEqualTo(2500);
  }

  @Test
  public void testEmptyJsonStringInit() throws InvalidProtocolBufferException {
    AstraConfigs.AstraConfig config =
        AstraConfig.fromJsonConfig(
            "{nodeRoles: [INDEX], "
                + "indexerConfig:{defaultQueryTimeoutMs:2500,serverConfig:{requestTimeoutMs:3000}}}");

    assertThat(config.getNodeRolesList().size()).isEqualTo(1);

    final AstraConfigs.KafkaConfig kafkaCfg = config.getIndexerConfig().getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEmpty();
    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEmpty();
    assertThat(kafkaCfg.getKafkaClientGroup()).isEmpty();
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEmpty();
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEmpty();
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEmpty();
    assertThat(kafkaCfg.getKafkaTopic()).isEmpty();

    final AstraConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEmpty();
    assertThat(s3Config.getS3SecretKey()).isEmpty();
    assertThat(s3Config.getS3Region()).isEmpty();
    assertThat(s3Config.getS3EndPoint()).isEmpty();

    final AstraConfigs.TracingConfig tracingConfig = config.getTracingConfig();
    assertThat(tracingConfig.getZipkinEndpoint()).isEmpty();
    assertThat(tracingConfig.getCommonTagsMap()).isEmpty();

    final AstraConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isZero();
    assertThat(indexerConfig.getMaxBytesPerChunk()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getCommitDurationSecs()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getRefreshDurationSecs()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getEnableFullTextSearch()).isFalse();
    assertThat(indexerConfig.getMaxChunksOnDisk()).isZero();
    assertThat(indexerConfig.getStaleDurationSecs()).isZero();
    assertThat(indexerConfig.getDataDirectory()).isEmpty();
    assertThat(indexerConfig.getDefaultQueryTimeoutMs()).isEqualTo(2500);
    assertThat(indexerConfig.getMaxOffsetDelayMessages()).isZero();
    assertThat(indexerConfig.getServerConfig().getServerPort()).isZero();
    assertThat(indexerConfig.getServerConfig().getServerAddress()).isEmpty();
    assertThat(indexerConfig.getServerConfig().getRequestTimeoutMs()).isEqualTo(3000);

    final AstraConfigs.QueryServiceConfig queryServiceConfig = config.getQueryConfig();
    assertThat(queryServiceConfig.getServerConfig().getServerPort()).isZero();
    assertThat(queryServiceConfig.getServerConfig().getServerAddress()).isEmpty();
    assertThat(queryServiceConfig.getManagerConnectString()).isEmpty();

    final AstraConfigs.MetadataStoreConfig metadataStoreConfig = config.getMetadataStoreConfig();
    final AstraConfigs.ZookeeperConfig zookeeperConfig = metadataStoreConfig.getZookeeperConfig();
    assertThat(zookeeperConfig.getZkConnectString()).isEmpty();
    assertThat(zookeeperConfig.getZkPathPrefix()).isEmpty();
    assertThat(zookeeperConfig.getZkSessionTimeoutMs()).isZero();
    assertThat(zookeeperConfig.getZkConnectionTimeoutMs()).isZero();
    assertThat(zookeeperConfig.getSleepBetweenRetriesMs()).isZero();

    final AstraConfigs.CacheConfig cacheConfig = config.getCacheConfig();
    final AstraConfigs.ServerConfig cacheServerConfig = cacheConfig.getServerConfig();
    assertThat(cacheConfig.getSlotsPerInstance()).isZero();
    assertThat(cacheConfig.getDataDirectory()).isEmpty();
    assertThat(cacheServerConfig.getServerPort()).isZero();
    assertThat(cacheServerConfig.getServerAddress()).isEmpty();

    final AstraConfigs.ManagerConfig managerConfig = config.getManagerConfig();
    assertThat(managerConfig.getEventAggregationSecs()).isZero();
    assertThat(managerConfig.getScheduleInitialDelayMins()).isZero();

    final AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        managerConfig.getReplicaCreationServiceConfig();
    assertThat(replicaCreationServiceConfig.getReplicaSetsCount()).isZero();
    assertThat(replicaCreationServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(replicaCreationServiceConfig.getReplicaLifespanMins()).isZero();

    final AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        managerConfig.getReplicaEvictionServiceConfig();
    assertThat(replicaEvictionServiceConfig.getSchedulePeriodMins()).isZero();

    final AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        managerConfig.getReplicaDeletionServiceConfig();
    assertThat(replicaDeletionServiceConfig.getSchedulePeriodMins()).isZero();

    final AstraConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            managerConfig.getRecoveryTaskAssignmentServiceConfig();
    assertThat(recoveryTaskAssignmentServiceConfig.getSchedulePeriodMins()).isZero();

    final AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        managerConfig.getReplicaAssignmentServiceConfig();
    assertThat(replicaAssignmentServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(replicaAssignmentServiceConfig.getReplicaSetsList()).isEmpty();
    assertThat(replicaAssignmentServiceConfig.getMaxConcurrentPerNode()).isZero();

    final AstraConfigs.ClusterConfig clusterConfig = config.getClusterConfig();
    assertThat(clusterConfig.getClusterName()).isEmpty();
    assertThat(clusterConfig.getEnv()).isEmpty();

    final AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRestoreServiceConfig =
        managerConfig.getReplicaRestoreServiceConfig();
    assertThat(replicaRestoreServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(replicaRestoreServiceConfig.getMaxReplicasPerRequest()).isZero();
    assertThat(replicaRestoreServiceConfig.getReplicaLifespanMins()).isZero();

    final AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        managerConfig.getSnapshotDeletionServiceConfig();
    assertThat(snapshotDeletionServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(snapshotDeletionServiceConfig.getSnapshotLifespanMins()).isZero();

    final AstraConfigs.ServerConfig managerServerConfig = managerConfig.getServerConfig();
    assertThat(managerServerConfig.getServerPort()).isZero();
    assertThat(managerServerConfig.getServerAddress()).isEmpty();

    final AstraConfigs.RecoveryConfig recoveryConfig = config.getRecoveryConfig();
    final AstraConfigs.ServerConfig recoveryServerConfig = recoveryConfig.getServerConfig();
    assertThat(recoveryServerConfig.getServerPort()).isZero();
    assertThat(recoveryServerConfig.getServerAddress()).isEmpty();

    final AstraConfigs.PreprocessorConfig preprocessorConfig = config.getPreprocessorConfig();
    assertThat(preprocessorConfig.getPreprocessorInstanceCount()).isZero();
    assertThat(preprocessorConfig.getRateLimiterMaxBurstSeconds()).isZero();

    final AstraConfigs.ServerConfig preprocessorServerConfig = preprocessorConfig.getServerConfig();
    assertThat(preprocessorServerConfig.getServerPort()).isZero();
    assertThat(preprocessorServerConfig.getServerAddress()).isEmpty();
  }

  @Test
  public void testEmptyYamlStringInit()
      throws InvalidProtocolBufferException, JsonProcessingException {
    String yamlCfgString =
        "nodeRoles: [INDEX]\n"
            + "indexerConfig:\n"
            + "  defaultQueryTimeoutMs: 2500\n"
            + "  serverConfig:\n"
            + "    requestTimeoutMs: 3000\n";
    AstraConfigs.AstraConfig config = AstraConfig.fromYamlConfig(yamlCfgString);

    assertThat(config.getNodeRolesList().size()).isEqualTo(1);

    final AstraConfigs.KafkaConfig kafkaCfg = config.getIndexerConfig().getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEmpty();
    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEmpty();
    assertThat(kafkaCfg.getKafkaClientGroup()).isEmpty();
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEmpty();
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEmpty();
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEmpty();
    assertThat(kafkaCfg.getKafkaTopic()).isEmpty();

    final AstraConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEmpty();
    assertThat(s3Config.getS3SecretKey()).isEmpty();
    assertThat(s3Config.getS3Region()).isEmpty();
    assertThat(s3Config.getS3EndPoint()).isEmpty();

    final AstraConfigs.TracingConfig tracingConfig = config.getTracingConfig();
    assertThat(tracingConfig.getZipkinEndpoint()).isEmpty();
    assertThat(tracingConfig.getCommonTagsMap()).isEmpty();
    assertThat(tracingConfig.getSamplingRate()).isEqualTo(0.0f);

    final AstraConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isZero();
    assertThat(indexerConfig.getMaxBytesPerChunk()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getCommitDurationSecs()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getRefreshDurationSecs()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getEnableFullTextSearch()).isFalse();
    assertThat(indexerConfig.getMaxChunksOnDisk()).isZero();
    assertThat(indexerConfig.getStaleDurationSecs()).isZero();
    assertThat(indexerConfig.getDataDirectory()).isEmpty();
    assertThat(indexerConfig.getMaxOffsetDelayMessages()).isZero();
    assertThat(indexerConfig.getServerConfig().getServerPort()).isZero();
    assertThat(indexerConfig.getServerConfig().getServerAddress()).isEmpty();

    final AstraConfigs.QueryServiceConfig queryServiceConfig = config.getQueryConfig();
    assertThat(queryServiceConfig.getServerConfig().getServerPort()).isZero();
    assertThat(queryServiceConfig.getServerConfig().getServerAddress()).isEmpty();
    assertThat(queryServiceConfig.getManagerConnectString()).isEmpty();

    final AstraConfigs.MetadataStoreConfig metadataStoreConfig = config.getMetadataStoreConfig();
    final AstraConfigs.ZookeeperConfig zookeeperConfig = metadataStoreConfig.getZookeeperConfig();
    assertThat(zookeeperConfig.getZkConnectString()).isEmpty();
    assertThat(zookeeperConfig.getZkPathPrefix()).isEmpty();
    assertThat(zookeeperConfig.getZkSessionTimeoutMs()).isZero();
    assertThat(zookeeperConfig.getZkConnectionTimeoutMs()).isZero();
    assertThat(zookeeperConfig.getSleepBetweenRetriesMs()).isZero();

    final AstraConfigs.CacheConfig cacheConfig = config.getCacheConfig();
    final AstraConfigs.ServerConfig cacheServerConfig = cacheConfig.getServerConfig();
    assertThat(cacheConfig.getSlotsPerInstance()).isZero();
    assertThat(cacheConfig.getDataDirectory()).isEmpty();
    assertThat(cacheServerConfig.getServerPort()).isZero();
    assertThat(cacheServerConfig.getServerAddress()).isEmpty();

    final AstraConfigs.ManagerConfig managerConfig = config.getManagerConfig();
    assertThat(managerConfig.getEventAggregationSecs()).isZero();
    assertThat(managerConfig.getScheduleInitialDelayMins()).isZero();

    final AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        managerConfig.getReplicaCreationServiceConfig();
    assertThat(replicaCreationServiceConfig.getReplicaSetsCount()).isZero();
    assertThat(replicaCreationServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(replicaCreationServiceConfig.getReplicaLifespanMins()).isZero();

    final AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        managerConfig.getReplicaEvictionServiceConfig();
    assertThat(replicaEvictionServiceConfig.getSchedulePeriodMins()).isZero();

    final AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        managerConfig.getReplicaDeletionServiceConfig();
    assertThat(replicaDeletionServiceConfig.getSchedulePeriodMins()).isZero();

    final AstraConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            managerConfig.getRecoveryTaskAssignmentServiceConfig();
    assertThat(recoveryTaskAssignmentServiceConfig.getSchedulePeriodMins()).isZero();

    final AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        managerConfig.getReplicaAssignmentServiceConfig();
    assertThat(replicaAssignmentServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(replicaAssignmentServiceConfig.getReplicaSetsList()).isEmpty();
    assertThat(replicaAssignmentServiceConfig.getMaxConcurrentPerNode()).isZero();

    final AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRestoreServiceConfig =
        managerConfig.getReplicaRestoreServiceConfig();
    assertThat(replicaRestoreServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(replicaRestoreServiceConfig.getMaxReplicasPerRequest()).isZero();
    assertThat(replicaRestoreServiceConfig.getReplicaLifespanMins()).isZero();

    final AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        managerConfig.getSnapshotDeletionServiceConfig();
    assertThat(snapshotDeletionServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(snapshotDeletionServiceConfig.getSnapshotLifespanMins()).isZero();

    final AstraConfigs.ServerConfig managerServerConfig = managerConfig.getServerConfig();
    assertThat(managerServerConfig.getServerPort()).isZero();
    assertThat(managerServerConfig.getServerAddress()).isEmpty();

    final AstraConfigs.PreprocessorConfig preprocessorConfig = config.getPreprocessorConfig();
    assertThat(preprocessorConfig.getPreprocessorInstanceCount()).isZero();
    assertThat(preprocessorConfig.getRateLimiterMaxBurstSeconds()).isZero();

    final AstraConfigs.ServerConfig preprocessorServerConfig = preprocessorConfig.getServerConfig();
    assertThat(preprocessorServerConfig.getServerPort()).isZero();
    assertThat(preprocessorServerConfig.getServerAddress()).isEmpty();
  }

  @Test
  public void testNodeRoleValidation() throws Exception {
    assertThatIllegalArgumentException().isThrownBy(() -> AstraConfig.fromYamlConfig("{}"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> AstraConfig.fromYamlConfig("nodeRoles: [INDEXER]"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> AstraConfig.fromYamlConfig("nodeRoles: [index]"));

    String yamlCfgString =
        "nodeRoles: [INDEX]\n"
            + "indexerConfig:\n"
            + "  defaultQueryTimeoutMs: 2500\n"
            + "  serverConfig:\n"
            + "    requestTimeoutMs: 3000\n"
            + "    serverPort: 8080\n"
            + "    serverAddress: localhost\n";
    List<AstraConfigs.NodeRole> roles =
        AstraConfig.fromYamlConfig(yamlCfgString).getNodeRolesList();
    assertThat(roles.size()).isEqualTo(1);
    assertThat(roles).containsExactly(AstraConfigs.NodeRole.INDEX);
  }

  @Test
  public void testBadDefaultQueryTimeoutMs() {

    final String yamlCfgString =
        "nodeRoles: [INDEX]\n"
            + "indexerConfig:\n"
            + "  defaultQueryTimeoutMs: 3500\n"
            + "  serverConfig:\n"
            + "    requestTimeoutMs: 3000\n"
            + "    serverPort: 8080\n"
            + "    serverAddress: localhost\n";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> AstraConfig.fromYamlConfig(yamlCfgString));

    final String yamlCfgString1 =
        "nodeRoles: [INDEX]\n"
            + "indexerConfig:\n"
            + "  defaultQueryTimeoutMs: 2500\n"
            + "  serverConfig:\n"
            + "    requestTimeoutMs: 2999\n"
            + "    serverPort: 8080\n"
            + "    serverAddress: localhost\n";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> AstraConfig.fromYamlConfig(yamlCfgString1));
  }

  @Test
  public void testMetadataStoreModeConfig()
      throws InvalidProtocolBufferException, JsonProcessingException {
    // We need to include proper server configs to pass validation
    String baseConfig =
        "indexerConfig:\n"
            + "  defaultQueryTimeoutMs: 2000\n"
            + "  serverConfig:\n"
            + "    requestTimeoutMs: 3000\n"
            + "    serverPort: 8080\n"
            + "    serverAddress: localhost\n";

    // Test YAML config with ZOOKEEPER_EXCLUSIVE mode (default value)
    String yamlConfig =
        "nodeRoles: [INDEX]\n"
            + baseConfig
            + "metadataStoreConfig:\n"
            + "  zookeeperConfig:\n"
            + "    zkConnectString: localhost:2181\n";
    AstraConfigs.AstraConfig config = AstraConfig.fromYamlConfig(yamlConfig);
    assertThat(config.getMetadataStoreConfig().getMode())
        .isEqualTo(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE);

    // Test YAML config with explicitly set ZOOKEEPER_EXCLUSIVE mode
    yamlConfig =
        "nodeRoles: [INDEX]\n"
            + baseConfig
            + "metadataStoreConfig:\n"
            + "  mode: ZOOKEEPER_EXCLUSIVE\n"
            + "  zookeeperConfig:\n"
            + "    zkConnectString: localhost:2181\n";
    config = AstraConfig.fromYamlConfig(yamlConfig);
    assertThat(config.getMetadataStoreConfig().getMode())
        .isEqualTo(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE);

    // Test YAML config with ETCD_EXCLUSIVE mode
    yamlConfig =
        "nodeRoles: [INDEX]\n"
            + baseConfig
            + "metadataStoreConfig:\n"
            + "  mode: ETCD_EXCLUSIVE\n"
            + "  zookeeperConfig:\n"
            + "    zkConnectString: localhost:2181\n";
    config = AstraConfig.fromYamlConfig(yamlConfig);
    assertThat(config.getMetadataStoreConfig().getMode())
        .isEqualTo(AstraConfigs.MetadataStoreMode.ETCD_EXCLUSIVE);

    // Test YAML config with BOTH_READ_ZOOKEEPER_WRITE mode
    yamlConfig =
        "nodeRoles: [INDEX]\n"
            + baseConfig
            + "metadataStoreConfig:\n"
            + "  mode: BOTH_READ_ZOOKEEPER_WRITE\n"
            + "  zookeeperConfig:\n"
            + "    zkConnectString: localhost:2181\n";
    config = AstraConfig.fromYamlConfig(yamlConfig);
    assertThat(config.getMetadataStoreConfig().getMode())
        .isEqualTo(AstraConfigs.MetadataStoreMode.BOTH_READ_ZOOKEEPER_WRITE);

    // Test YAML config with BOTH_READ_ETCD_WRITE mode
    yamlConfig =
        "nodeRoles: [INDEX]\n"
            + baseConfig
            + "metadataStoreConfig:\n"
            + "  mode: BOTH_READ_ETCD_WRITE\n"
            + "  zookeeperConfig:\n"
            + "    zkConnectString: localhost:2181\n";
    config = AstraConfig.fromYamlConfig(yamlConfig);
    assertThat(config.getMetadataStoreConfig().getMode())
        .isEqualTo(AstraConfigs.MetadataStoreMode.BOTH_READ_ETCD_WRITE);

    // Test JSON config with different modes
    String jsonConfig =
        "{\"nodeRoles\": [\"INDEX\"], "
            + "\"indexerConfig\": {\"defaultQueryTimeoutMs\": 2000, \"serverConfig\": {\"requestTimeoutMs\": 3000, \"serverPort\": 8080, \"serverAddress\": \"localhost\"}}, "
            + "\"metadataStoreConfig\": {\"mode\": \"BOTH_READ_ETCD_WRITE\"}}";
    config = AstraConfig.fromJsonConfig(jsonConfig);
    assertThat(config.getMetadataStoreConfig().getMode())
        .isEqualTo(AstraConfigs.MetadataStoreMode.BOTH_READ_ETCD_WRITE);
  }
}
