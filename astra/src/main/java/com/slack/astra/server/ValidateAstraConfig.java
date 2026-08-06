package com.slack.astra.server;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.proto.config.AstraConfigs;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.util.Strings;

public class ValidateAstraConfig {

  /**
   * ValidateConfig ensures that various config values across classes are consistent. The class
   * using a config is still expected to ensure the config values are valid. For example, the roles
   * can't be empty.
   */
  public static void validateConfig(AstraConfigs.AstraConfig AstraConfig) {
    validateNodeRoles(AstraConfig.getNodeRolesList());
    if (AstraConfig.getNodeRolesList().contains(AstraConfigs.NodeRole.INDEX)) {
      validateIndexConfig(AstraConfig.getIndexerConfig());
    }
    if (AstraConfig.getNodeRolesList().contains(AstraConfigs.NodeRole.QUERY)) {
      validateQueryConfig(AstraConfig.getQueryConfig());
    }
    if (AstraConfig.getNodeRolesList().contains(AstraConfigs.NodeRole.CACHE)) {
      validateCacheConfig(AstraConfig.getCacheConfig());
    }
    validateMetadataStoreConfig(AstraConfig.getMetadataStoreConfig());
  }

  private static void validateMetadataStoreConfig(
      AstraConfigs.MetadataStoreConfig metadataStoreConfig) {
    boolean anyDynamo =
        metadataStoreConfig.getStoreModesMap().values().stream()
            .anyMatch(mode -> mode == AstraConfigs.MetadataStoreMode.DYNAMODB_CREATES);
    if (anyDynamo) {
      AstraConfigs.DynamoDbConfig dynamodbConfig = metadataStoreConfig.getDynamodbConfig();
      checkArgument(
          metadataStoreConfig.hasDynamodbConfig() && dynamodbConfig.getEnabled(),
          "A store is set to DYNAMODB_CREATES but dynamodbConfig is missing or not enabled");
      checkArgument(
          Strings.isNotBlank(dynamodbConfig.getTableName()),
          "dynamodbConfig.tableName is required when a store uses DYNAMODB_CREATES");
    }
  }

  private static void validateIndexConfig(AstraConfigs.IndexerConfig indexerConfig) {
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

  private static void validateQueryConfig(AstraConfigs.QueryServiceConfig queryConfig) {
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

  private static void validateCacheConfig(AstraConfigs.CacheConfig cacheConfig) {
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

  public static void validateNodeRoles(List<AstraConfigs.NodeRole> nodeRoleList) {
    // We don't need further checks for node roles since JSON parsing will throw away roles not part
    // of the enum
    checkArgument(
        !nodeRoleList.isEmpty(),
        "Astra must start with at least 1 node role. Accepted roles are "
            + Arrays.toString(AstraConfigs.NodeRole.values()));
  }
}
