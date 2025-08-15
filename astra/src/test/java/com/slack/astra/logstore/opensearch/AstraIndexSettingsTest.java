package com.slack.astra.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.config.AstraConfigs;
import org.junit.jupiter.api.Test;
import org.opensearch.index.IndexSettings;

public class AstraIndexSettingsTest {

  @Test
  public void testDefaultFieldWithNullLuceneConfig() {
    IndexSettings indexSettings = AstraIndexSettings.getInstance(null);
    String defaultField = indexSettings.getSettings().get("index.query.default_field");
    assertThat(defaultField).isEqualTo(LogMessage.SystemField.ALL.fieldName);
  }

  @Test
  public void testDefaultFieldWithFullTextSearchEnabled() {
    AstraConfigs.LuceneConfig luceneConfig =
        AstraConfigs.LuceneConfig.newBuilder().setEnableFullTextSearch(true).build();

    IndexSettings indexSettings = AstraIndexSettings.getInstance(luceneConfig);
    String defaultField = indexSettings.getSettings().get("index.query.default_field");
    assertThat(defaultField).isEqualTo(LogMessage.SystemField.ALL.fieldName);
  }

  @Test
  public void testDefaultFieldWithFullTextSearchDisabled() {
    AstraConfigs.LuceneConfig luceneConfig =
        AstraConfigs.LuceneConfig.newBuilder().setEnableFullTextSearch(false).build();

    IndexSettings indexSettings = AstraIndexSettings.getInstance(luceneConfig);
    String defaultField = indexSettings.getSettings().get("index.query.default_field");
    assertThat(defaultField).isEqualTo("*");
  }

  @Test
  public void testDefaultInstanceStillWorks() {
    IndexSettings indexSettings = AstraIndexSettings.getInstance();
    String defaultField = indexSettings.getSettings().get("index.query.default_field");
    assertThat(defaultField).isEqualTo(LogMessage.SystemField.ALL.fieldName);
  }
}
