package com.slack.kaldb.testlib;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.proto.config.KaldbConfigs;

public class KaldbConfigUtil {
  // Initialize kaldb config with empty for tests.
  public static KaldbConfigs.KaldbConfig initEmptyConfig() throws InvalidProtocolBufferException {
    return KaldbConfig.initFromJsonStr("{}");
  }
}
