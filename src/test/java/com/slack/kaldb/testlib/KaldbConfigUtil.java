package com.slack.kaldb.testlib;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.config.KaldbConfig;

public class KaldbConfigUtil {
  // Initialize kaldb config with empty for tests.
  public static void initEmptyConfig() throws InvalidProtocolBufferException {
    KaldbConfig.initFromJsonStr("{}");
  }
}
