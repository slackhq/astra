package com.slack.astra.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.proto.config.AstraConfigs;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;

/**
 * AstraConfig contains the config params used in the project.
 *
 * <p>TODO: Set reasonable defaults for the config values.
 */
public class AstraConfig {

  // This should be either moved to a proper config, or likely completely rethought.
  // This doesn't make sense as a global for all services, as each service has potentially different
  // requirements
  // Default start/stop duration for guava services.
  @Deprecated public static Duration DEFAULT_START_STOP_DURATION = Duration.ofSeconds(15);

  // This should go away, in factor of using the zkConnectionTimeoutMs config value
  @Deprecated public static final int DEFAULT_ZK_TIMEOUT_SECS = 30;

  // This should either be a static final closer to the chunk manager, a config value, or removed
  // entirely
  @Deprecated public static final String CHUNK_DATA_PREFIX = "log";

  private static AstraConfig _instance = null;

  // Parse a json string as a AstraConfig proto struct.
  @VisibleForTesting
  static AstraConfigs.AstraConfig fromJsonConfig(String jsonStr)
      throws InvalidProtocolBufferException {
    AstraConfigs.AstraConfig.Builder AstraConfigBuilder = AstraConfigs.AstraConfig.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(jsonStr, AstraConfigBuilder);
    AstraConfigs.AstraConfig AstraConfig = AstraConfigBuilder.build();
    ValidateAstraConfig.validateConfig(AstraConfig);
    return AstraConfig;
  }

  // Parse a yaml string as a AstraConfig proto struct
  @VisibleForTesting
  public static AstraConfigs.AstraConfig fromYamlConfig(String yamlStr)
      throws InvalidProtocolBufferException, JsonProcessingException {
    return fromYamlConfig(yamlStr, System::getenv);
  }

  @VisibleForTesting
  public static AstraConfigs.AstraConfig fromYamlConfig(
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

  private static void initFromConfigObject(AstraConfigs.AstraConfig config) {
    _instance = new AstraConfig(config);
  }

  public static AstraConfigs.AstraConfig get() {
    return _instance.config;
  }

  private final AstraConfigs.AstraConfig config;

  private AstraConfig(AstraConfigs.AstraConfig config) {
    this.config = config;
  }
}
