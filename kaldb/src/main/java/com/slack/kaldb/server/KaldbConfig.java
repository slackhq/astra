package com.slack.kaldb.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;

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
    ValidateKaldbConfig.validateConfig(kaldbConfig);
    return kaldbConfig;
  }

  // Parse a yaml string as a KaldbConfig proto struct
  @VisibleForTesting
  public static KaldbConfigs.KaldbConfig fromYamlConfig(String yamlStr)
      throws InvalidProtocolBufferException, JsonProcessingException {
    return fromYamlConfig(yamlStr, System::getenv);
  }

  @VisibleForTesting
  public static KaldbConfigs.KaldbConfig fromYamlConfig(
      String yamlStr, StringLookup variableResolver)
      throws InvalidProtocolBufferException, JsonProcessingException {
    StringSubstitutor substitute = new StringSubstitutor(variableResolver);
    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    ObjectMapper jsonWriter = new ObjectMapper();

    Object obj = yamlReader.readValue(substitute.replace(yamlStr), Object.class);
    return fromJsonConfig(jsonWriter.writeValueAsString(obj));
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
