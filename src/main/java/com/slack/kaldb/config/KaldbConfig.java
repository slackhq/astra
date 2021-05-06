package com.slack.kaldb.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * KaldbConfig contains the config params used in the project.
 *
 * <p>TODO: Set reasonable defaults for the config values.
 */
public class KaldbConfig {
  private static KaldbConfig _instance = null;

  // Parse a json string as a KaldbConfig proto struct.
  @VisibleForTesting
  static KaldbConfigs.KaldbConfig fromJsonConfig(String jsonStr)
      throws InvalidProtocolBufferException {
    KaldbConfigs.KaldbConfig.Builder kaldbConfigBuilder = KaldbConfigs.KaldbConfig.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(jsonStr, kaldbConfigBuilder);
    return kaldbConfigBuilder.build();
  }

  @VisibleForTesting
  public static void reset() {
    _instance = null;
  }

  public static void initFromFile(Path cfgFilePath) throws IOException {
    if (_instance == null) {
      if (Files.notExists(cfgFilePath)) {
        throw new IllegalArgumentException(
            "Missing config file at: " + cfgFilePath.toAbsolutePath().toString());
      }

      initFromJsonStr(Files.readString(cfgFilePath));
    }
  }

  public static void initFromJsonStr(String jsonCfgString) throws InvalidProtocolBufferException {
    initFromConfigObject(fromJsonConfig(jsonCfgString));
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
