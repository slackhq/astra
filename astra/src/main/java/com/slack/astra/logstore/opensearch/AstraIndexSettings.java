package com.slack.astra.logstore.opensearch;

import static org.opensearch.common.settings.IndexScopedSettings.BUILT_IN_INDEX_SETTINGS;

import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.config.AstraConfigs;
import java.util.HashSet;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;

public class AstraIndexSettings {
  private static IndexSettings defaultIndexSettings = null;

  // we can make this configurable when SchemaAwareLogDocumentBuilderImpl enforces a limit
  // set this to a high number for now
  private static final int TOTAL_FIELDS_LIMIT =
      Integer.parseInt(System.getProperty("astra.mapping.totalFieldsLimit", "2500"));

  private AstraIndexSettings() {}

  public static IndexSettings getInstance() {
    if (defaultIndexSettings == null) {
      defaultIndexSettings = buildIndexSettings(null);
    }
    return defaultIndexSettings;
  }

  public static IndexSettings getInstance(AstraConfigs.LuceneConfig luceneConfig) {
    return buildIndexSettings(luceneConfig);
  }

  /** Builds the minimal amount of IndexSettings required for using Aggregations */
  private static IndexSettings buildIndexSettings(AstraConfigs.LuceneConfig luceneConfig) {
    Settings settings =
        Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_2_11_0)
            .put(
                MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), TOTAL_FIELDS_LIMIT)

            // Astra time sorts the indexes while building it
            // {LuceneIndexStoreImpl#buildIndexWriterConfig}
            // When we were using the lucene query parser the sort info was leveraged by lucene
            // automatically ( as the sort info persists in the segment info ) at query time.
            // However the OpenSearch query parser has a custom implementation which relies on the
            // index sort info to be present as a setting here.
            .put("index.sort.field", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
            .put("index.sort.order", "desc")
            .put("index.query.default_field", getDefaultField(luceneConfig))
            .put("index.query_string.lenient", false)
            .build();

    Settings nodeSetings =
        Settings.builder().put("indices.query.query_string.analyze_wildcard", true).build();

    IndexScopedSettings indexScopedSettings =
        new IndexScopedSettings(settings, new HashSet<>(BUILT_IN_INDEX_SETTINGS));

    return new IndexSettings(
        IndexMetadata.builder("index").settings(settings).build(),
        nodeSetings,
        indexScopedSettings);
  }

  private static String getDefaultField(AstraConfigs.LuceneConfig luceneConfig) {
    if (luceneConfig != null && !luceneConfig.getEnableFullTextSearch()) {
      return "*";
    }
    return LogMessage.SystemField.ALL.fieldName;
  }
}
