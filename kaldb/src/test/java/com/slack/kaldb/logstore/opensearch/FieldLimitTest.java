package com.slack.kaldb.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.slack.kaldb.logstore.opensearch.OpenSearchAdapter;
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
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(openSearchAdapter::reloadSchema);

    OpenSearchAdapter.TOTAL_FIELDS_LIMIT = 2;
    openSearchAdapter = new OpenSearchAdapter(getChunkSchema(2));
    assertThatNoException().isThrownBy(openSearchAdapter::reloadSchema);
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
