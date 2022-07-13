package com.slack.kaldb.server;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.writer.LogMessageTransformer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.apache.commons.text.StringSubstitutor;

/**
 * KaldbConfig contains the config params used in the project.
 *
 * <p>TODO: Set reasonable defaults for the config values.
 */
public class KaldbConfig {
  // Default start/stop duration for guava services.
  public static Duration DEFAULT_START_STOP_DURATION = Duration.ofSeconds(15);
  public static final int DEFAULT_ZK_TIMEOUT_SECS = 15;

  public static final String CHUNK_DATA_PREFIX = "log";

  private static KaldbConfig _instance = null;

  // Parse a json string as a KaldbConfig proto struct.
  @VisibleForTesting
  static KaldbConfigs.KaldbConfig fromJsonConfig(String jsonStr)
      throws InvalidProtocolBufferException {
    KaldbConfigs.KaldbConfig.Builder kaldbConfigBuilder = KaldbConfigs.KaldbConfig.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(jsonStr, kaldbConfigBuilder);
    KaldbConfigs.KaldbConfig kaldbConfig = kaldbConfigBuilder.build();
    validateConfig(kaldbConfig);
    return kaldbConfig;
  }

  // Parse a yaml string as a KaldbConfig proto struct
  @VisibleForTesting
  static KaldbConfigs.KaldbConfig fromYamlConfig(String yamlStr)
      throws InvalidProtocolBufferException, JsonProcessingException {
    StringSubstitutor substitute = new StringSubstitutor(System::getenv);
    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    ObjectMapper jsonWriter = new ObjectMapper();

    Object obj = yamlReader.readValue(substitute.replace(yamlStr), Object.class);
    return fromJsonConfig(jsonWriter.writeValueAsString(obj));
  }

  /**
   * ValidateConfig ensures that various config values across classes are consistent. The class
   * using a config is still expected to ensure the config values are valid. For example, the roles
   * can't be empty.
   */
  public static void validateConfig(KaldbConfigs.KaldbConfig kaldbConfig) {
    validateNodeRoles(kaldbConfig.getNodeRolesList());
    kaldbConfig
        .getNodeRolesList()
        .stream()
        .map(role -> getServerConfigForRole(kaldbConfig, role))
        .forEach(serverConfig -> validateRequestTimeout(serverConfig.getRequestTimeoutMs()));
    if (kaldbConfig.getNodeRolesList().contains(KaldbConfigs.NodeRole.INDEX)) {
      validateDataTransformerConfig(kaldbConfig.getIndexerConfig().getDataTransformer());
      validateQueryTimeouts(
          kaldbConfig.getIndexerConfig().getServerConfig().getRequestTimeoutMs(),
          kaldbConfig.getIndexerConfig().getDefaultQueryTimeoutMs());
    }
    if (kaldbConfig.getNodeRolesList().contains(KaldbConfigs.NodeRole.QUERY)) {
      validateQueryTimeouts(
          kaldbConfig.getQueryConfig().getServerConfig().getRequestTimeoutMs(),
          kaldbConfig.getQueryConfig().getDefaultQueryTimeoutMs());
    }
    if (kaldbConfig.getNodeRolesList().contains(KaldbConfigs.NodeRole.CACHE)) {
      validateQueryTimeouts(
          kaldbConfig.getCacheConfig().getServerConfig().getRequestTimeoutMs(),
          kaldbConfig.getCacheConfig().getDefaultQueryTimeoutMs());
    }
  }

  private static void validateQueryTimeouts(int requestTimeoutMs, int defaultQueryTimeoutMs) {
    checkArgument(defaultQueryTimeoutMs > 0, "defaultQueryTimeoutMs cannot be 0");
    checkArgument(
        defaultQueryTimeoutMs < requestTimeoutMs,
        "query service timeout cannot be lower than global request timeout");
  }

  private static void validateRequestTimeout(int requestTimeoutMs) {
    checkArgument(requestTimeoutMs > 0, "requestTimeoutMs cannot be 0");
    checkArgument(requestTimeoutMs < 3000, "Kaldb request timeouts must be atleast 3000ms");
  }

  private static KaldbConfigs.ServerConfig getServerConfigForRole(
      KaldbConfigs.KaldbConfig kaldbConfig, KaldbConfigs.NodeRole role) {
    if (role.equals(KaldbConfigs.NodeRole.INDEX)) {
      return kaldbConfig.getIndexerConfig().getServerConfig();
    }
    // TODO add for other components. Not a huuge fan since this means every new component needs to
    // rememebr this
    return kaldbConfig.getIndexerConfig().getServerConfig();
  }

  public static void validateNodeRoles(List<KaldbConfigs.NodeRole> nodeRoleList) {
    // We don't need further checks for node roles since JSON parsing will throw away roles not part
    // of the enum
    checkArgument(
        !nodeRoleList.isEmpty(),
        "Kaldb must start with at least 1 node role. Accepted roles are "
            + Arrays.toString(KaldbConfigs.NodeRole.values()));
  }

  @VisibleForTesting
  public static final Map<String, LogMessageTransformer> INDEXER_DATA_TRANSFORMER_MAP =
      ImmutableMap.of(
          "api_log",
          LogMessageWriterImpl.apiLogTransformer,
          "trace_span",
          LogMessageWriterImpl.traceSpanTransformer);

  public static void validateDataTransformerConfig(String dataTransformerConfig) {
    checkArgument(
        dataTransformerConfig != null && !dataTransformerConfig.isEmpty(),
        "IndexerConfig can't have an empty dataTransformer config.");
    checkArgument(
        INDEXER_DATA_TRANSFORMER_MAP.containsKey(dataTransformerConfig),
        "Invalid data transformer config: " + dataTransformerConfig);
  }

  @VisibleForTesting
  static void reset() {
    _instance = null;
  }

  public static void initFromFile(Path cfgFilePath) throws IOException {
    if (_instance == null) {
      if (Files.notExists(cfgFilePath)) {
        throw new IllegalArgumentException(
            "Missing config file at: " + cfgFilePath.toAbsolutePath());
      }

      String filename = cfgFilePath.getFileName().toString();
      if (filename.endsWith(".yaml")) {
        initFromYamlStr(Files.readString(cfgFilePath));
      } else if (filename.endsWith(".json")) {
        initFromJsonStr(Files.readString(cfgFilePath));
      } else {
        throw new RuntimeException(
            "Invalid config file format provided - must be either .json or .yaml");
      }
    }
  }

  private static void initFromJsonStr(String jsonCfgString) throws InvalidProtocolBufferException {
    initFromConfigObject(fromJsonConfig(jsonCfgString));
  }

  private static void initFromYamlStr(String yamlString)
      throws InvalidProtocolBufferException, JsonProcessingException {
    initFromConfigObject(fromYamlConfig(yamlString));
  }

  private static void initFromConfigObject(KaldbConfigs.KaldbConfig config) {
    _instance = new KaldbConfig(config);
  }

  static KaldbConfigs.KaldbConfig get() {
    return _instance.config;
  }

  private final KaldbConfigs.KaldbConfig config;

  private KaldbConfig(KaldbConfigs.KaldbConfig config) {
    this.config = config;
  }
}
