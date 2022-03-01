package com.slack.kaldb.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
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

  // TODO: Pass this in via config file.
  public static final String CHUNK_DATA_PREFIX = "log";
  public static final long DEFAULT_ROLLOVER_FUTURE_TIMEOUT_MS = 30000;

  // Timeouts are structured such that we always attempt to return a successful response, as we
  // include metadata that should always be present. The Armeria timeout is used at the top request,
  // distributed query is used as a deadline for all nodes to return, and the local query timeout
  // is used for controlling lucene future timeouts.
  public static final Duration ARMERIA_TIMEOUT_DURATION = Duration.of(15, ChronoUnit.SECONDS);
  public static Duration DISTRIBUTED_QUERY_TIMEOUT_DURATION =
      ARMERIA_TIMEOUT_DURATION.minus(Duration.of(2, ChronoUnit.SECONDS));
  public static Duration LOCAL_QUERY_TIMEOUT_DURATION =
      DISTRIBUTED_QUERY_TIMEOUT_DURATION.minus(Duration.of(2, ChronoUnit.SECONDS));

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

  public static void validateConfig(KaldbConfigs.KaldbConfig kaldbConfig) {
    // We don't need further checks for node roles since JSON parsing will throw away roles not part
    // of the enum
    Preconditions.checkArgument(
        !kaldbConfig.getNodeRolesList().isEmpty(),
        "Kaldb must start with atleast 1 node role. Accepted roles are "
            + Arrays.toString(KaldbConfigs.NodeRole.values()));
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

  @VisibleForTesting
  public static void reset() {
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

  public static void initFromJsonStr(String jsonCfgString) throws InvalidProtocolBufferException {
    initFromConfigObject(fromJsonConfig(jsonCfgString));
  }

  public static void initFromYamlStr(String yamlString)
      throws InvalidProtocolBufferException, JsonProcessingException {
    initFromConfigObject(fromYamlConfig(yamlString));
  }

  public static void initFromConfigObject(KaldbConfigs.KaldbConfig config) {
    _instance = new KaldbConfig(config);
  }

  public static KaldbConfigs.KaldbConfig get() {
    return _instance.config;
  }

  private final KaldbConfigs.KaldbConfig config;

  private KaldbConfig(KaldbConfigs.KaldbConfig config) {
    this.config = config;
  }
}
