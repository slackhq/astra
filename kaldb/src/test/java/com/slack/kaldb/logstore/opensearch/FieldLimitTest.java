package com.slack.kaldb.logstore.opensearch;

import static com.slack.kaldb.logstore.opensearch.OpenSearchAdapter.tryRegisterField;
import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.metadata.schema.FieldType;
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
    Map<String, LuceneFieldDef> fieldDefMap = getChunkSchema(2);
    OpenSearchAdapter openSearchAdapter = new OpenSearchAdapter(fieldDefMap);
    assertThat(registerFields(openSearchAdapter, fieldDefMap)).isFalse();

    OpenSearchAdapter.TOTAL_FIELDS_LIMIT = 2;
    fieldDefMap = getChunkSchema(2);
    openSearchAdapter = new OpenSearchAdapter(fieldDefMap);
    assertThat(registerFields(openSearchAdapter, fieldDefMap)).isTrue();
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

  private boolean registerFields(
      OpenSearchAdapter openSearchAdapter, Map<String, LuceneFieldDef> chunkSchema) {
    // only int fields supported here for simplicity
    for (Map.Entry<String, LuceneFieldDef> entry : chunkSchema.entrySet()) {
      if (entry.getValue().fieldType == FieldType.INTEGER) {
        boolean success =
            tryRegisterField(
                openSearchAdapter.mapperService,
                entry.getValue().name,
                b -> b.field("type", "text"));
        if (!success) {
          return false;
        }
      }
    }
    return true;
  }
}
