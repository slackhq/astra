package com.slack.kaldb.server;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.proto.config.KaldbConfigs;
import java.util.Arrays;
import java.util.List;

public class ValidateKaldbConfig {

  /**
   * ValidateConfig ensures that various config values across classes are consistent. The class
   * using a config is still expected to ensure the config values are valid. For example, the roles
   * can't be empty.
   */
  public static void validateConfig(KaldbConfigs.KaldbConfig kaldbConfig) {
    validateNodeRoles(kaldbConfig.getNodeRolesList());
    if (kaldbConfig.getNodeRolesList().contains(KaldbConfigs.NodeRole.INDEX)) {
      validateIndexConfig(kaldbConfig.getIndexerConfig());
    }
    if (kaldbConfig.getNodeRolesList().contains(KaldbConfigs.NodeRole.QUERY)) {
      validateQueryConfig(kaldbConfig.getQueryConfig());
    }
    if (kaldbConfig.getNodeRolesList().contains(KaldbConfigs.NodeRole.CACHE)) {
      validateCacheConfig(kaldbConfig.getCacheConfig());
    }
  }

  private static void validateIndexConfig(KaldbConfigs.IndexerConfig indexerConfig) {
    checkArgument(
        indexerConfig.getServerConfig().getRequestTimeoutMs() >= 3000,
        "IndexerConfig requestTimeoutMs cannot less than 3000ms");
    checkArgument(
        indexerConfig.getDefaultQueryTimeoutMs() >= 1000,
        "IndexerConfig defaultQueryTimeoutMs cannot less than 1000ms");
    checkArgument(
        indexerConfig.getServerConfig().getRequestTimeoutMs()
            > indexerConfig.getDefaultQueryTimeoutMs(),
        "IndexerConfig requestTimeoutMs must be higher than defaultQueryTimeoutMs");
  }

  private static void validateQueryConfig(KaldbConfigs.QueryServiceConfig queryConfig) {
    checkArgument(
        queryConfig.getServerConfig().getRequestTimeoutMs() >= 3000,
        "QueryConfig requestTimeoutMs cannot less than 3000ms");
    checkArgument(
        queryConfig.getDefaultQueryTimeoutMs() >= 1000,
        "QueryConfig defaultQueryTimeoutMs cannot less than 1000ms");
    checkArgument(
        queryConfig.getServerConfig().getRequestTimeoutMs()
            > queryConfig.getDefaultQueryTimeoutMs(),
        "QueryConfig requestTimeoutMs must be higher than defaultQueryTimeoutMs");
  }

  private static void validateCacheConfig(KaldbConfigs.CacheConfig cacheConfig) {
    checkArgument(
        cacheConfig.getServerConfig().getRequestTimeoutMs() >= 3000,
        "CacheConfig requestTimeoutMs cannot less than 3000ms");
    checkArgument(
        cacheConfig.getDefaultQueryTimeoutMs() >= 1000,
        "CacheConfig defaultQueryTimeoutMs cannot less than 1000ms");
    checkArgument(
        cacheConfig.getServerConfig().getRequestTimeoutMs()
            > cacheConfig.getDefaultQueryTimeoutMs(),
        "CacheConfig requestTimeoutMs must be higher than defaultQueryTimeoutMs");
  }

  public static void validateNodeRoles(List<KaldbConfigs.NodeRole> nodeRoleList) {
    // We don't need further checks for node roles since JSON parsing will throw away roles not part
    // of the enum
    checkArgument(
        !nodeRoleList.isEmpty(),
        "Kaldb must start with at least 1 node role. Accepted roles are "
            + Arrays.toString(KaldbConfigs.NodeRole.values()));
  }
}
