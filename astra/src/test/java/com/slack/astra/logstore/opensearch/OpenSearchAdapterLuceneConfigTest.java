package com.slack.astra.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.proto.config.AstraConfigs;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.opensearch.index.IndexSettings;

/**
 * Test to verify that OpenSearchAdapter correctly uses LuceneConfig settings and that the behavior
 * is consistent between different components.
 */
public class OpenSearchAdapterLuceneConfigTest {

  @Test
  public void testOpenSearchAdapterWithFullTextSearchEnabled() throws Exception {
    AstraConfigs.LuceneConfig luceneConfig =
        AstraConfigs.LuceneConfig.newBuilder().setEnableFullTextSearch(true).build();

    ConcurrentHashMap<String, LuceneFieldDef> chunkSchema = new ConcurrentHashMap<>();
    OpenSearchAdapter adapter = new OpenSearchAdapter(chunkSchema, luceneConfig);

    IndexSettings indexSettings = extractIndexSettings(adapter);
    String defaultField = indexSettings.getSettings().get("index.query.default_field");

    assertThat(defaultField).isEqualTo(LogMessage.SystemField.ALL.fieldName);
  }

  @Test
  public void testOpenSearchAdapterWithFullTextSearchDisabled() throws Exception {
    AstraConfigs.LuceneConfig luceneConfig =
        AstraConfigs.LuceneConfig.newBuilder().setEnableFullTextSearch(false).build();

    ConcurrentHashMap<String, LuceneFieldDef> chunkSchema = new ConcurrentHashMap<>();
    OpenSearchAdapter adapter = new OpenSearchAdapter(chunkSchema, luceneConfig);

    IndexSettings indexSettings = extractIndexSettings(adapter);
    String defaultField = indexSettings.getSettings().get("index.query.default_field");

    assertThat(defaultField).isEqualTo("*");
  }

  @Test
  public void testOpenSearchAdapterWithoutLuceneConfig() throws Exception {
    ConcurrentHashMap<String, LuceneFieldDef> chunkSchema = new ConcurrentHashMap<>();
    OpenSearchAdapter adapter = new OpenSearchAdapter(chunkSchema);

    IndexSettings indexSettings = extractIndexSettings(adapter);
    String defaultField = indexSettings.getSettings().get("index.query.default_field");

    assertThat(defaultField).isEqualTo(LogMessage.SystemField.ALL.fieldName);
  }

  @Test
  public void testLogIndexSearcherImplWithLuceneConfig() throws Exception {
    // Test LogIndexSearcherImpl directly to ensure it passes LuceneConfig correctly
    AstraConfigs.LuceneConfig disabledConfig =
        AstraConfigs.LuceneConfig.newBuilder().setEnableFullTextSearch(false).build();

    ConcurrentHashMap<String, LuceneFieldDef> chunkSchema = new ConcurrentHashMap<>();

    // Create a mock AstraSearcherManager - we can't easily instantiate it, so we'll test at the
    // OpenSearchAdapter level
    OpenSearchAdapter adapterWithDisabledConfig =
        new OpenSearchAdapter(chunkSchema, disabledConfig);
    IndexSettings indexSettingsDisabled = extractIndexSettings(adapterWithDisabledConfig);
    String defaultFieldDisabled =
        indexSettingsDisabled.getSettings().get("index.query.default_field");

    OpenSearchAdapter adapterWithoutConfig = new OpenSearchAdapter(chunkSchema);
    IndexSettings indexSettingsDefault = extractIndexSettings(adapterWithoutConfig);
    String defaultFieldDefault =
        indexSettingsDefault.getSettings().get("index.query.default_field");

    // Verify they are different
    assertThat(defaultFieldDisabled).isEqualTo("*");
    assertThat(defaultFieldDefault).isEqualTo(LogMessage.SystemField.ALL.fieldName);
    assertThat(defaultFieldDisabled).isNotEqualTo(defaultFieldDefault);
  }

  @Test
  public void testConsistentBehaviorAcrossMultipleInstances() throws Exception {
    AstraConfigs.LuceneConfig enabledConfig =
        AstraConfigs.LuceneConfig.newBuilder().setEnableFullTextSearch(true).build();

    AstraConfigs.LuceneConfig disabledConfig =
        AstraConfigs.LuceneConfig.newBuilder().setEnableFullTextSearch(false).build();

    ConcurrentHashMap<String, LuceneFieldDef> chunkSchema = new ConcurrentHashMap<>();

    // Create multiple instances with the same config
    for (int i = 0; i < 3; i++) {
      OpenSearchAdapter enabledAdapter = new OpenSearchAdapter(chunkSchema, enabledConfig);
      IndexSettings enabledSettings = extractIndexSettings(enabledAdapter);
      String enabledField = enabledSettings.getSettings().get("index.query.default_field");
      assertThat(enabledField).isEqualTo(LogMessage.SystemField.ALL.fieldName);

      OpenSearchAdapter disabledAdapter = new OpenSearchAdapter(chunkSchema, disabledConfig);
      IndexSettings disabledSettings = extractIndexSettings(disabledAdapter);
      String disabledField = disabledSettings.getSettings().get("index.query.default_field");
      assertThat(disabledField).isEqualTo("*");
    }
  }

  /** Extract IndexSettings from OpenSearchAdapter using reflection */
  private IndexSettings extractIndexSettings(OpenSearchAdapter adapter) throws Exception {
    Field indexSettingsField = OpenSearchAdapter.class.getDeclaredField("indexSettings");
    indexSettingsField.setAccessible(true);
    return (IndexSettings) indexSettingsField.get(adapter);
  }
}
