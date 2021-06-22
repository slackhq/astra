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

  // Parse a json string as a KaldbConfig proto struct.
  @VisibleForTesting
  public static KaldbConfigs.KaldbConfig fromJsonConfig(String jsonStr)
      throws InvalidProtocolBufferException {
    KaldbConfigs.KaldbConfig.Builder kaldbConfigBuilder = KaldbConfigs.KaldbConfig.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(jsonStr, kaldbConfigBuilder);
    return kaldbConfigBuilder.build();
  }

  public static KaldbConfigs.KaldbConfig initFromFile(Path cfgFilePath) throws IOException {
    if (Files.notExists(cfgFilePath)) {
      throw new IllegalArgumentException(
          "Missing config file at: " + cfgFilePath.toAbsolutePath().toString());
    }
    return initFromJsonStr(Files.readString(cfgFilePath));
  }

  public static KaldbConfigs.KaldbConfig initFromJsonStr(String jsonCfgString)
      throws InvalidProtocolBufferException {
    return fromJsonConfig(jsonCfgString);
  }
}
