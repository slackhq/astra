package com.slack.kaldb.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Test;

public class FieldLimitTest {

  @Test
  public void checkIfMaxFieldLimitsIsEnforced() {
    Map<String, LuceneFieldDef> fieldDefMap = getChunkSchema(5001);
    OpenSearchAdapter openSearchAdapter = new OpenSearchAdapter(fieldDefMap);
    assertThat(openSearchAdapter.registerNewFields(fieldDefMap)).isFalse();

    fieldDefMap = getChunkSchema(2);
    openSearchAdapter = new OpenSearchAdapter(fieldDefMap);
    assertThat(openSearchAdapter.registerNewFields(fieldDefMap)).isTrue();
  }

  private Map<String, LuceneFieldDef> getChunkSchema(int nFields) {
    final Map<String, LuceneFieldDef> fieldDefMap = new ConcurrentHashMap<>();
    for (int i = 0; i < nFields; i++) {
      String name = "field" + i;
      LuceneFieldDef fieldDef = new LuceneFieldDef(name, "integer", true, true, true);
      fieldDefMap.put(name, fieldDef);
    }
    return fieldDefMap;
  }
}
