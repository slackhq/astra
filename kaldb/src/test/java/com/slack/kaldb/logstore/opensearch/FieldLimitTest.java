package com.slack.kaldb.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class FieldLimitTest {

  @Before
  public void resetTotalFieldsLimit() {
    OpenSearchAdapter.TOTAL_FIELDS_LIMIT = 5000;
  }

  @AfterClass
  public static void resetTotalFieldsLimitAfterTestRuns() {
    OpenSearchAdapter.TOTAL_FIELDS_LIMIT = 5000;
  }

  @Test
  public void checkIfMaxFieldLimitsIsEnforced() {
    OpenSearchAdapter.TOTAL_FIELDS_LIMIT = 1;
    OpenSearchAdapter openSearchAdapter = new OpenSearchAdapter(getChunkSchema(2));
    assertThat(openSearchAdapter.reloadSchema()).isFalse();

    OpenSearchAdapter.TOTAL_FIELDS_LIMIT = 2;
    openSearchAdapter = new OpenSearchAdapter(getChunkSchema(2));
    assertThat(openSearchAdapter.reloadSchema()).isTrue();
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
