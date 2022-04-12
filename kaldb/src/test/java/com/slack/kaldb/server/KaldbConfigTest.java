package com.slack.kaldb.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KaldbConfigTest {

  @Before
  public void setUp() {
    KaldbConfig.reset();
  }

  @After
  public void tearDown() {
    KaldbConfig.reset();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInitWithMissingConfigFile() throws IOException {
    KaldbConfig.initFromFile(Path.of("missing_config_file.json"));
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void testEmptyJsonCfgFile() throws InvalidProtocolBufferException {
    KaldbConfig.fromJsonConfig("");
  }

  @Test
  public void testMissingDataTransformerConfig() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode indexerConfig =
        mapper.createObjectNode().put("maxMessagesPerChunk", 1).put("maxBytesPerChunk", 100);
    ObjectNode node = mapper.createObjectNode();
    node.set("nodeRoles", mapper.createArrayNode().add("INDEX"));
    node.set("indexerConfig", indexerConfig);
    final String missingRequiredField =
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> KaldbConfig.fromJsonConfig(missingRequiredField));
  }

  @Test
  public void testIntToStrTypeConversionForWrongJsonType()
      throws InvalidProtocolBufferException, JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode indexerConfig =
        mapper
            .createObjectNode()
            .put("maxMessagesPerChunk", 1)
            .put("maxBytesPerChunk", 100)
            .put("dataTransformer", "api_log");
    ObjectNode kafkaConfig =
        mapper.createObjectNode().put("kafkaTopicPartition", 1).put("kafkaSessionTimeout", 30000);
    ObjectNode node = mapper.createObjectNode();
    node.set("nodeRoles", mapper.createArrayNode().add("INDEX"));
    node.set("indexerConfig", indexerConfig);
    node.set("kafkaConfig", kafkaConfig);
    final String missingRequiredField =
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);

    final KaldbConfigs.KaldbConfig kalDbConfig = KaldbConfig.fromJsonConfig(missingRequiredField);

    final KaldbConfigs.KafkaConfig kafkaCfg = kalDbConfig.getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("1");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");
  }

  @Test
  public void testStrToIntTypeConversionForWrongJsonType()
      throws InvalidProtocolBufferException, JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode indexerConfig =
        mapper
            .createObjectNode()
            .put("maxMessagesPerChunk", 1)
            .put("maxBytesPerChunk", 100)
            .put("dataTransformer", "api_log");
    ObjectNode node = mapper.createObjectNode();
    node.set("nodeRoles", mapper.createArrayNode().add("INDEX"));
    node.set("indexerConfig", indexerConfig);
    final String missingRequiredField =
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);

    final KaldbConfigs.KaldbConfig kalDbConfig = KaldbConfig.fromJsonConfig(missingRequiredField);

    final KaldbConfigs.IndexerConfig indexerCfg = kalDbConfig.getIndexerConfig();
    assertThat(indexerCfg.getMaxMessagesPerChunk()).isEqualTo(1);
    assertThat(indexerCfg.getMaxBytesPerChunk()).isEqualTo(100);
  }

  @Test
  public void testIgnoreExtraConfigField() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode indexerConfig =
        mapper
            .createObjectNode()
            .put("maxMessagesPerChunk", 1)
            .put("maxBytesPerChunk", 100)
            .put("dataTransformer", "api_log")
            .put("ignoredField", "ignore");
    ObjectNode kafkaConfig =
        mapper
            .createObjectNode()
            .put("kafkaTopicPartition", 1)
            .put("kafkaSessionTimeout", 30000)
            .put("ignoreExtraField", "ignoredField");
    ObjectNode node = mapper.createObjectNode();
    node.set("nodeRoles", mapper.createArrayNode().add("INDEX"));
    node.set("indexerConfig", indexerConfig);
    node.set("kafkaConfig", kafkaConfig);

    final String configWithExtraField =
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);

    final KaldbConfigs.KaldbConfig kalDbConfig = KaldbConfig.fromJsonConfig(configWithExtraField);

    final KaldbConfigs.KafkaConfig kafkaCfg = kalDbConfig.getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("1");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");
    assertThat(kafkaCfg.getKafkaTopic()).isEmpty();

    final KaldbConfigs.IndexerConfig indexerCfg = kalDbConfig.getIndexerConfig();
    assertThat(indexerCfg.getMaxMessagesPerChunk()).isEqualTo(1);
    assertThat(indexerCfg.getMaxBytesPerChunk()).isEqualTo(100);
    assertThat(kalDbConfig.getNodeRolesList()).containsExactly(KaldbConfigs.NodeRole.INDEX);

    final KaldbConfigs.S3Config s3Config = kalDbConfig.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEmpty();
    assertThat(s3Config.getS3SecretKey()).isEmpty();
    assertThat(s3Config.getS3Region()).isEmpty();
    assertThat(s3Config.getS3EndPoint()).isEmpty();
  }

  @Test
  public void testParseKaldbJsonConfigFile() throws IOException {
    final File cfgFile =
        new File(getClass().getClassLoader().getResource("test_config.json").getFile());
    KaldbConfig.initFromFile(cfgFile.toPath());
    final KaldbConfigs.KaldbConfig config = KaldbConfig.get();

    assertThat(config).isNotNull();

    assertThat(config.getNodeRolesList().size()).isEqualTo(4);
    assertThat(config.getNodeRolesList().get(0)).isEqualTo(KaldbConfigs.NodeRole.INDEX);
    assertThat(config.getNodeRolesList().get(1)).isEqualTo(KaldbConfigs.NodeRole.QUERY);
    assertThat(config.getNodeRolesList().get(2)).isEqualTo(KaldbConfigs.NodeRole.CACHE);
    assertThat(config.getNodeRolesList().get(3)).isEqualTo(KaldbConfigs.NodeRole.MANAGER);

    final KaldbConfigs.KafkaConfig kafkaCfg = config.getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopic()).isEqualTo("testTopic");
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("1");
    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEqualTo("kafka.us-east-1.consul:9094");
    assertThat(kafkaCfg.getKafkaClientGroup()).isEqualTo("kaldb-test");
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEqualTo("true");
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEqualTo("5000");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");

    final KaldbConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEqualTo("access");
    assertThat(s3Config.getS3SecretKey()).isEqualTo("secret");
    assertThat(s3Config.getS3Region()).isEqualTo("us-east-1");
    assertThat(s3Config.getS3EndPoint()).isEqualTo("https://s3.us-east-1.amazonaws.com/");
    assertThat(s3Config.getS3Bucket()).isEqualTo("test-s3-bucket");

    final KaldbConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isEqualTo(1000);
    assertThat(indexerConfig.getMaxBytesPerChunk()).isEqualTo(100000);
    assertThat(indexerConfig.getLuceneConfig().getCommitDurationSecs()).isEqualTo(10);
    assertThat(indexerConfig.getLuceneConfig().getRefreshDurationSecs()).isEqualTo(11);
    assertThat(indexerConfig.getStaleDurationSecs()).isEqualTo(7200);
    assertThat(indexerConfig.getDataTransformer()).isEqualTo("api_log");
    assertThat(indexerConfig.getDataDirectory()).isEqualTo("/tmp");
    assertThat(indexerConfig.getServerConfig().getServerPort()).isEqualTo(8080);
    assertThat(indexerConfig.getServerConfig().getServerAddress()).isEqualTo("localhost");
    assertThat(indexerConfig.getMaxOffsetDelayMessages()).isEqualTo(10002);

    final KaldbConfigs.QueryServiceConfig queryServiceConfig = config.getQueryConfig();
    assertThat(queryServiceConfig.getServerConfig().getServerPort()).isEqualTo(8081);
    assertThat(queryServiceConfig.getServerConfig().getServerAddress()).isEqualTo("1.2.3.4");

    final KaldbConfigs.MetadataStoreConfig metadataStoreConfig = config.getMetadataStoreConfig();
    final KaldbConfigs.ZookeeperConfig zookeeperConfig = metadataStoreConfig.getZookeeperConfig();
    assertThat(zookeeperConfig.getZkConnectString()).isEqualTo("1.2.3.4:9092");
    assertThat(zookeeperConfig.getZkPathPrefix()).isEqualTo("zkPrefix");
    assertThat(zookeeperConfig.getZkSessionTimeoutMs()).isEqualTo(1000);
    assertThat(zookeeperConfig.getZkConnectionTimeoutMs()).isEqualTo(1500);
    assertThat(zookeeperConfig.getSleepBetweenRetriesMs()).isEqualTo(500);

    final KaldbConfigs.CacheConfig cacheConfig = config.getCacheConfig();
    final KaldbConfigs.ServerConfig cacheServerConfig = cacheConfig.getServerConfig();
    assertThat(cacheConfig.getSlotsPerInstance()).isEqualTo(10);
    assertThat(cacheConfig.getDataDirectory()).isEqualTo("/tmp");
    assertThat(cacheServerConfig.getServerPort()).isEqualTo(8082);
    assertThat(cacheServerConfig.getServerAddress()).isEqualTo("localhost");

    final KaldbConfigs.ManagerConfig managerConfig = config.getManagerConfig();
    assertThat(managerConfig.getEventAggregationSecs()).isEqualTo(10);
    assertThat(managerConfig.getScheduleInitialDelayMins()).isEqualTo(1);

    final KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        managerConfig.getReplicaCreationServiceConfig();
    assertThat(replicaCreationServiceConfig.getReplicasPerSnapshot()).isEqualTo(2);
    assertThat(replicaCreationServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(replicaCreationServiceConfig.getReplicaLifespanMins()).isEqualTo(1440);

    final KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        managerConfig.getReplicaEvictionServiceConfig();
    assertThat(replicaEvictionServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        managerConfig.getReplicaDeletionServiceConfig();
    assertThat(replicaDeletionServiceConfig.getSchedulePeriodMins()).isEqualTo(90);

    final KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            managerConfig.getRecoveryTaskAssignmentServiceConfig();
    assertThat(recoveryTaskAssignmentServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        managerConfig.getReplicaAssignmentServiceConfig();
    assertThat(replicaAssignmentServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        managerConfig.getSnapshotDeletionServiceConfig();
    assertThat(snapshotDeletionServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(snapshotDeletionServiceConfig.getSnapshotLifespanMins()).isEqualTo(10080);

    final KaldbConfigs.ServerConfig managerServerConfig = managerConfig.getServerConfig();
    assertThat(managerServerConfig.getServerPort()).isEqualTo(8083);
    assertThat(managerServerConfig.getServerAddress()).isEqualTo("localhost");

    final KaldbConfigs.RecoveryConfig recoveryConfig = config.getRecoveryConfig();
    final KaldbConfigs.ServerConfig recoveryServerConfig = recoveryConfig.getServerConfig();
    assertThat(recoveryServerConfig.getServerPort()).isEqualTo(8084);
    assertThat(recoveryServerConfig.getServerAddress()).isEqualTo("localhost");

    final KaldbConfigs.PreprocessorConfig preprocessorConfig = config.getPreprocessorConfig();
    assertThat(preprocessorConfig.getPreprocessorInstanceCount()).isEqualTo(1);
    assertThat(preprocessorConfig.getUpstreamTopicsList()).isEqualTo(List.of("test-topic"));
    assertThat(preprocessorConfig.getDownstreamTopic()).isEqualTo("test-topic-out");
    assertThat(preprocessorConfig.getDataTransformer()).isEqualTo("api_log");

    final KaldbConfigs.ServerConfig preprocessorServerConfig = preprocessorConfig.getServerConfig();
    assertThat(preprocessorServerConfig.getServerPort()).isEqualTo(8085);
    assertThat(preprocessorServerConfig.getServerAddress()).isEqualTo("localhost");

    final KaldbConfigs.PreprocessorConfig.KafkaStreamConfig preprocessorKafkaStreamConfig =
        preprocessorConfig.getKafkaStreamConfig();
    assertThat(preprocessorKafkaStreamConfig.getBootstrapServers()).isEqualTo("localhost:9092");
    assertThat(preprocessorKafkaStreamConfig.getApplicationId()).isEqualTo("kaldb_preprocessor");
    assertThat(preprocessorKafkaStreamConfig.getNumStreamThreads()).isEqualTo(2);
    assertThat(preprocessorKafkaStreamConfig.getProcessingGuarantee()).isEqualTo("at_least_once");
  }

  @Test
  public void testParseKaldbYamlConfigFile() throws IOException {
    final File cfgFile =
        new File(getClass().getClassLoader().getResource("test_config.yaml").getFile());

    KaldbConfig.initFromFile(cfgFile.toPath());
    final KaldbConfigs.KaldbConfig config = KaldbConfig.get();

    assertThat(config).isNotNull();

    assertThat(config.getNodeRolesList().size()).isEqualTo(4);
    assertThat(config.getNodeRolesList().get(0)).isEqualTo(KaldbConfigs.NodeRole.INDEX);
    assertThat(config.getNodeRolesList().get(1)).isEqualTo(KaldbConfigs.NodeRole.QUERY);
    assertThat(config.getNodeRolesList().get(2)).isEqualTo(KaldbConfigs.NodeRole.CACHE);
    assertThat(config.getNodeRolesList().get(3)).isEqualTo(KaldbConfigs.NodeRole.MANAGER);

    final KaldbConfigs.KafkaConfig kafkaCfg = config.getKafkaConfig();

    // todo - for testing env var substitution we could use something like Mockito (or similar) in
    // the future
    assertThat(kafkaCfg.getKafkaTopic()).isEqualTo("test-topic");

    // uses default fallback as we expect the env var NOT_PRESENT to not be set
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("0");

    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEqualTo("localhost:9092");
    assertThat(kafkaCfg.getKafkaClientGroup()).isEqualTo("kaldb-test");
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEqualTo("true");
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEqualTo("5000");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");

    final KaldbConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEqualTo("access");
    assertThat(s3Config.getS3SecretKey()).isEqualTo("secret");
    assertThat(s3Config.getS3Region()).isEqualTo("us-east-1");
    assertThat(s3Config.getS3EndPoint()).isEqualTo("localhost:9090");
    assertThat(s3Config.getS3Bucket()).isEqualTo("test-s3-bucket");

    final KaldbConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isEqualTo(100);
    assertThat(indexerConfig.getMaxBytesPerChunk()).isEqualTo(100000);
    assertThat(indexerConfig.getLuceneConfig().getCommitDurationSecs()).isEqualTo(10);
    assertThat(indexerConfig.getLuceneConfig().getRefreshDurationSecs()).isEqualTo(11);
    assertThat(indexerConfig.getStaleDurationSecs()).isEqualTo(7200);
    assertThat(indexerConfig.getDataTransformer()).isEqualTo("api_log");
    assertThat(indexerConfig.getDataDirectory()).isEqualTo("/tmp");
    assertThat(indexerConfig.getMaxOffsetDelayMessages()).isEqualTo(10001);
    assertThat(indexerConfig.getServerConfig().getServerPort()).isEqualTo(8080);
    assertThat(indexerConfig.getServerConfig().getServerAddress()).isEqualTo("localhost");

    final KaldbConfigs.QueryServiceConfig readConfig = config.getQueryConfig();
    assertThat(readConfig.getServerConfig().getServerPort()).isEqualTo(8081);
    assertThat(readConfig.getServerConfig().getServerAddress()).isEqualTo("1.2.3.4");

    final KaldbConfigs.MetadataStoreConfig metadataStoreConfig = config.getMetadataStoreConfig();
    final KaldbConfigs.ZookeeperConfig zookeeperConfig = metadataStoreConfig.getZookeeperConfig();
    assertThat(zookeeperConfig.getZkConnectString()).isEqualTo("1.2.3.4:9092");
    assertThat(zookeeperConfig.getZkPathPrefix()).isEqualTo("zkPrefix");
    assertThat(zookeeperConfig.getZkSessionTimeoutMs()).isEqualTo(1000);
    assertThat(zookeeperConfig.getZkConnectionTimeoutMs()).isEqualTo(1500);
    assertThat(zookeeperConfig.getSleepBetweenRetriesMs()).isEqualTo(500);

    final KaldbConfigs.CacheConfig cacheConfig = config.getCacheConfig();
    final KaldbConfigs.ServerConfig cacheServerConfig = cacheConfig.getServerConfig();
    assertThat(cacheConfig.getSlotsPerInstance()).isEqualTo(10);
    assertThat(cacheServerConfig.getServerPort()).isEqualTo(8082);
    assertThat(cacheConfig.getDataDirectory()).isEqualTo("/tmp");
    assertThat(cacheServerConfig.getServerAddress()).isEqualTo("localhost");

    final KaldbConfigs.ManagerConfig managerConfig = config.getManagerConfig();
    assertThat(managerConfig.getEventAggregationSecs()).isEqualTo(10);
    assertThat(managerConfig.getScheduleInitialDelayMins()).isEqualTo(1);

    final KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        managerConfig.getReplicaCreationServiceConfig();
    assertThat(replicaCreationServiceConfig.getReplicasPerSnapshot()).isEqualTo(2);
    assertThat(replicaCreationServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(replicaCreationServiceConfig.getReplicaLifespanMins()).isEqualTo(1440);

    final KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        managerConfig.getReplicaEvictionServiceConfig();
    assertThat(replicaEvictionServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        managerConfig.getReplicaDeletionServiceConfig();
    assertThat(replicaDeletionServiceConfig.getSchedulePeriodMins()).isEqualTo(90);

    final KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            managerConfig.getRecoveryTaskAssignmentServiceConfig();
    assertThat(recoveryTaskAssignmentServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        managerConfig.getReplicaAssignmentServiceConfig();
    assertThat(replicaAssignmentServiceConfig.getSchedulePeriodMins()).isEqualTo(10);

    final KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        managerConfig.getSnapshotDeletionServiceConfig();
    assertThat(snapshotDeletionServiceConfig.getSchedulePeriodMins()).isEqualTo(15);
    assertThat(snapshotDeletionServiceConfig.getSnapshotLifespanMins()).isEqualTo(10080);

    final KaldbConfigs.ServerConfig managerServerConfig = managerConfig.getServerConfig();
    assertThat(managerServerConfig.getServerPort()).isEqualTo(8083);
    assertThat(managerServerConfig.getServerAddress()).isEqualTo("localhost");

    final KaldbConfigs.RecoveryConfig recoveryConfig = config.getRecoveryConfig();
    final KaldbConfigs.ServerConfig recoveryServerConfig = recoveryConfig.getServerConfig();
    assertThat(recoveryServerConfig.getServerPort()).isEqualTo(8084);
    assertThat(recoveryServerConfig.getServerAddress()).isEqualTo("localhost");

    final KaldbConfigs.PreprocessorConfig preprocessorConfig = config.getPreprocessorConfig();
    assertThat(preprocessorConfig.getPreprocessorInstanceCount()).isEqualTo(1);
    assertThat(preprocessorConfig.getUpstreamTopicsList()).isEqualTo(List.of("test-topic"));
    assertThat(preprocessorConfig.getDownstreamTopic()).isEqualTo("test-topic-out");
    assertThat(preprocessorConfig.getDataTransformer()).isEqualTo("api_log");

    final KaldbConfigs.ServerConfig preprocessorServerConfig = preprocessorConfig.getServerConfig();
    assertThat(preprocessorServerConfig.getServerPort()).isEqualTo(8085);
    assertThat(preprocessorServerConfig.getServerAddress()).isEqualTo("localhost");

    final KaldbConfigs.PreprocessorConfig.KafkaStreamConfig preprocessorKafkaStreamConfig =
        preprocessorConfig.getKafkaStreamConfig();
    assertThat(preprocessorKafkaStreamConfig.getBootstrapServers()).isEqualTo("localhost:9092");
    assertThat(preprocessorKafkaStreamConfig.getApplicationId()).isEqualTo("kaldb_preprocessor");
    assertThat(preprocessorKafkaStreamConfig.getNumStreamThreads()).isEqualTo(2);
    assertThat(preprocessorKafkaStreamConfig.getProcessingGuarantee()).isEqualTo("at_least_once");
  }

  @Test(expected = RuntimeException.class)
  public void testParseFormats() throws IOException {
    // only json/yaml file extentions are supported
    KaldbConfig.initFromFile(Path.of("README.md"));
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void testMalformedYaml() throws InvalidProtocolBufferException, JsonProcessingException {
    KaldbConfig.fromYamlConfig(":test");
  }

  @Test
  public void testMissingDataTransformerConfigForCache() throws InvalidProtocolBufferException {
    KaldbConfigs.KaldbConfig config = KaldbConfig.fromJsonConfig("{nodeRoles: [CACHE]}");

    assertThat(config.getNodeRolesList().size()).isEqualTo(1);
    assertThat(config.getNodeRolesList()).containsOnly(KaldbConfigs.NodeRole.CACHE);
    final KaldbConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getDataTransformer()).isEmpty();
  }

  @Test
  public void testEmptyJsonStringInit() throws InvalidProtocolBufferException {
    KaldbConfigs.KaldbConfig config =
        KaldbConfig.fromJsonConfig(
            "{nodeRoles: [INDEX], " + "indexerConfig:{dataTransformer:api_log}}");

    assertThat(config.getNodeRolesList().size()).isEqualTo(1);

    final KaldbConfigs.KafkaConfig kafkaCfg = config.getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEmpty();
    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEmpty();
    assertThat(kafkaCfg.getKafkaClientGroup()).isEmpty();
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEmpty();
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEmpty();
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEmpty();
    assertThat(kafkaCfg.getKafkaTopic()).isEmpty();

    final KaldbConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEmpty();
    assertThat(s3Config.getS3SecretKey()).isEmpty();
    assertThat(s3Config.getS3Region()).isEmpty();
    assertThat(s3Config.getS3EndPoint()).isEmpty();

    final KaldbConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isZero();
    assertThat(indexerConfig.getMaxBytesPerChunk()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getCommitDurationSecs()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getRefreshDurationSecs()).isZero();
    assertThat(indexerConfig.getStaleDurationSecs()).isZero();
    assertThat(indexerConfig.getDataDirectory()).isEmpty();
    assertThat(indexerConfig.getDataTransformer()).isEqualTo("api_log");
    assertThat(indexerConfig.getMaxOffsetDelayMessages()).isZero();
    assertThat(indexerConfig.getServerConfig().getServerPort()).isZero();
    assertThat(indexerConfig.getServerConfig().getServerAddress()).isEmpty();

    final KaldbConfigs.QueryServiceConfig queryServiceConfig = config.getQueryConfig();
    assertThat(queryServiceConfig.getServerConfig().getServerPort()).isZero();
    assertThat(queryServiceConfig.getServerConfig().getServerAddress()).isEmpty();

    final KaldbConfigs.MetadataStoreConfig metadataStoreConfig = config.getMetadataStoreConfig();
    final KaldbConfigs.ZookeeperConfig zookeeperConfig = metadataStoreConfig.getZookeeperConfig();
    assertThat(zookeeperConfig.getZkConnectString()).isEmpty();
    assertThat(zookeeperConfig.getZkPathPrefix()).isEmpty();
    assertThat(zookeeperConfig.getZkSessionTimeoutMs()).isZero();
    assertThat(zookeeperConfig.getZkConnectionTimeoutMs()).isZero();
    assertThat(zookeeperConfig.getSleepBetweenRetriesMs()).isZero();

    final KaldbConfigs.CacheConfig cacheConfig = config.getCacheConfig();
    final KaldbConfigs.ServerConfig cacheServerConfig = cacheConfig.getServerConfig();
    assertThat(cacheConfig.getSlotsPerInstance()).isZero();
    assertThat(cacheConfig.getDataDirectory()).isEmpty();
    assertThat(cacheServerConfig.getServerPort()).isZero();
    assertThat(cacheServerConfig.getServerAddress()).isEmpty();

    final KaldbConfigs.ManagerConfig managerConfig = config.getManagerConfig();
    assertThat(managerConfig.getEventAggregationSecs()).isZero();
    assertThat(managerConfig.getScheduleInitialDelayMins()).isZero();

    final KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        managerConfig.getReplicaCreationServiceConfig();
    assertThat(replicaCreationServiceConfig.getReplicasPerSnapshot()).isZero();
    assertThat(replicaCreationServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(replicaCreationServiceConfig.getReplicaLifespanMins()).isZero();

    final KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        managerConfig.getReplicaEvictionServiceConfig();
    assertThat(replicaEvictionServiceConfig.getSchedulePeriodMins()).isZero();

    final KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        managerConfig.getReplicaDeletionServiceConfig();
    assertThat(replicaDeletionServiceConfig.getSchedulePeriodMins()).isZero();

    final KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            managerConfig.getRecoveryTaskAssignmentServiceConfig();
    assertThat(recoveryTaskAssignmentServiceConfig.getSchedulePeriodMins()).isZero();

    final KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        managerConfig.getReplicaAssignmentServiceConfig();
    assertThat(replicaAssignmentServiceConfig.getSchedulePeriodMins()).isZero();

    final KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        managerConfig.getSnapshotDeletionServiceConfig();
    assertThat(snapshotDeletionServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(snapshotDeletionServiceConfig.getSnapshotLifespanMins()).isZero();

    final KaldbConfigs.ServerConfig managerServerConfig = managerConfig.getServerConfig();
    assertThat(managerServerConfig.getServerPort()).isZero();
    assertThat(managerServerConfig.getServerAddress()).isEmpty();

    final KaldbConfigs.RecoveryConfig recoveryConfig = config.getRecoveryConfig();
    final KaldbConfigs.ServerConfig recoveryServerConfig = recoveryConfig.getServerConfig();
    assertThat(recoveryServerConfig.getServerPort()).isZero();
    assertThat(recoveryServerConfig.getServerAddress()).isEmpty();

    final KaldbConfigs.PreprocessorConfig preprocessorConfig = config.getPreprocessorConfig();
    assertThat(preprocessorConfig.getPreprocessorInstanceCount()).isZero();
    assertThat(preprocessorConfig.getUpstreamTopicsList()).isEmpty();
    assertThat(preprocessorConfig.getDownstreamTopic()).isEmpty();
    assertThat(preprocessorConfig.getDataTransformer()).isEmpty();

    final KaldbConfigs.ServerConfig preprocessorServerConfig = preprocessorConfig.getServerConfig();
    assertThat(preprocessorServerConfig.getServerPort()).isZero();
    assertThat(preprocessorServerConfig.getServerAddress()).isEmpty();

    final KaldbConfigs.PreprocessorConfig.KafkaStreamConfig preprocessorKafkaStreamConfig =
        preprocessorConfig.getKafkaStreamConfig();
    assertThat(preprocessorKafkaStreamConfig.getBootstrapServers()).isEmpty();
    assertThat(preprocessorKafkaStreamConfig.getApplicationId()).isEmpty();
    assertThat(preprocessorKafkaStreamConfig.getNumStreamThreads()).isZero();
    assertThat(preprocessorKafkaStreamConfig.getProcessingGuarantee()).isEmpty();
  }

  @Test
  public void testEmptyYamlStringInit()
      throws InvalidProtocolBufferException, JsonProcessingException {
    String yamlCfgString =
        "nodeRoles: [INDEX]\n" + "indexerConfig:\n" + "  dataTransformer: api_log";
    KaldbConfigs.KaldbConfig config = KaldbConfig.fromYamlConfig(yamlCfgString);

    assertThat(config.getNodeRolesList().size()).isEqualTo(1);

    final KaldbConfigs.KafkaConfig kafkaCfg = config.getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEmpty();
    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEmpty();
    assertThat(kafkaCfg.getKafkaClientGroup()).isEmpty();
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEmpty();
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEmpty();
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEmpty();
    assertThat(kafkaCfg.getKafkaTopic()).isEmpty();

    final KaldbConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEmpty();
    assertThat(s3Config.getS3SecretKey()).isEmpty();
    assertThat(s3Config.getS3Region()).isEmpty();
    assertThat(s3Config.getS3EndPoint()).isEmpty();

    final KaldbConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isZero();
    assertThat(indexerConfig.getMaxBytesPerChunk()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getCommitDurationSecs()).isZero();
    assertThat(indexerConfig.getLuceneConfig().getRefreshDurationSecs()).isZero();
    assertThat(indexerConfig.getStaleDurationSecs()).isZero();
    assertThat(indexerConfig.getDataDirectory()).isEmpty();
    assertThat(indexerConfig.getDataTransformer()).isEqualTo("api_log");
    assertThat(indexerConfig.getMaxOffsetDelayMessages()).isZero();
    assertThat(indexerConfig.getServerConfig().getServerPort()).isZero();
    assertThat(indexerConfig.getServerConfig().getServerAddress()).isEmpty();

    final KaldbConfigs.QueryServiceConfig queryServiceConfig = config.getQueryConfig();
    assertThat(queryServiceConfig.getServerConfig().getServerPort()).isZero();
    assertThat(queryServiceConfig.getServerConfig().getServerAddress()).isEmpty();

    final KaldbConfigs.MetadataStoreConfig metadataStoreConfig = config.getMetadataStoreConfig();
    final KaldbConfigs.ZookeeperConfig zookeeperConfig = metadataStoreConfig.getZookeeperConfig();
    assertThat(zookeeperConfig.getZkConnectString()).isEmpty();
    assertThat(zookeeperConfig.getZkPathPrefix()).isEmpty();
    assertThat(zookeeperConfig.getZkSessionTimeoutMs()).isZero();
    assertThat(zookeeperConfig.getZkConnectionTimeoutMs()).isZero();
    assertThat(zookeeperConfig.getSleepBetweenRetriesMs()).isZero();

    final KaldbConfigs.CacheConfig cacheConfig = config.getCacheConfig();
    final KaldbConfigs.ServerConfig cacheServerConfig = cacheConfig.getServerConfig();
    assertThat(cacheConfig.getSlotsPerInstance()).isZero();
    assertThat(cacheConfig.getDataDirectory()).isEmpty();
    assertThat(cacheServerConfig.getServerPort()).isZero();
    assertThat(cacheServerConfig.getServerAddress()).isEmpty();

    final KaldbConfigs.ManagerConfig managerConfig = config.getManagerConfig();
    assertThat(managerConfig.getEventAggregationSecs()).isZero();
    assertThat(managerConfig.getScheduleInitialDelayMins()).isZero();

    final KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        managerConfig.getReplicaCreationServiceConfig();
    assertThat(replicaCreationServiceConfig.getReplicasPerSnapshot()).isZero();
    assertThat(replicaCreationServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(replicaCreationServiceConfig.getReplicaLifespanMins()).isZero();

    final KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        managerConfig.getReplicaEvictionServiceConfig();
    assertThat(replicaEvictionServiceConfig.getSchedulePeriodMins()).isZero();

    final KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        managerConfig.getReplicaDeletionServiceConfig();
    assertThat(replicaDeletionServiceConfig.getSchedulePeriodMins()).isZero();

    final KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            managerConfig.getRecoveryTaskAssignmentServiceConfig();
    assertThat(recoveryTaskAssignmentServiceConfig.getSchedulePeriodMins()).isZero();

    final KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        managerConfig.getReplicaAssignmentServiceConfig();
    assertThat(replicaAssignmentServiceConfig.getSchedulePeriodMins()).isZero();

    final KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        managerConfig.getSnapshotDeletionServiceConfig();
    assertThat(snapshotDeletionServiceConfig.getSchedulePeriodMins()).isZero();
    assertThat(snapshotDeletionServiceConfig.getSnapshotLifespanMins()).isZero();

    final KaldbConfigs.ServerConfig managerServerConfig = managerConfig.getServerConfig();
    assertThat(managerServerConfig.getServerPort()).isZero();
    assertThat(managerServerConfig.getServerAddress()).isEmpty();

    final KaldbConfigs.PreprocessorConfig preprocessorConfig = config.getPreprocessorConfig();
    assertThat(preprocessorConfig.getPreprocessorInstanceCount()).isZero();
    assertThat(preprocessorConfig.getUpstreamTopicsList()).isEmpty();
    assertThat(preprocessorConfig.getDownstreamTopic()).isEmpty();
    assertThat(preprocessorConfig.getDataTransformer()).isEmpty();

    final KaldbConfigs.ServerConfig preprocessorServerConfig = preprocessorConfig.getServerConfig();
    assertThat(preprocessorServerConfig.getServerPort()).isZero();
    assertThat(preprocessorServerConfig.getServerAddress()).isEmpty();

    final KaldbConfigs.PreprocessorConfig.KafkaStreamConfig preprocessorKafkaStreamConfig =
        preprocessorConfig.getKafkaStreamConfig();
    assertThat(preprocessorKafkaStreamConfig.getBootstrapServers()).isEmpty();
    assertThat(preprocessorKafkaStreamConfig.getApplicationId()).isEmpty();
    assertThat(preprocessorKafkaStreamConfig.getNumStreamThreads()).isZero();
    assertThat(preprocessorKafkaStreamConfig.getProcessingGuarantee()).isEmpty();
  }

  @Test
  public void testNodeRoleValidation() throws Exception {
    assertThatIllegalArgumentException().isThrownBy(() -> KaldbConfig.fromYamlConfig("{}"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> KaldbConfig.fromYamlConfig("nodeRoles: [INDEXER]"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> KaldbConfig.fromYamlConfig("nodeRoles: [index]"));

    String yamlCfgString =
        "nodeRoles: [INDEX]\n" + "indexerConfig:\n" + "  dataTransformer: api_log";
    List<KaldbConfigs.NodeRole> roles =
        KaldbConfig.fromYamlConfig(yamlCfgString).getNodeRolesList();
    assertThat(roles.size()).isEqualTo(1);
    assertThat(roles).containsExactly(KaldbConfigs.NodeRole.INDEX);
  }
}
