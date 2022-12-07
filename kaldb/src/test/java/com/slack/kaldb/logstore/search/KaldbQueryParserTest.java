package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MessageUtil.TEST_MESSAGE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class KaldbQueryParserTest {

  @Rule
  public TemporaryLogStoreAndSearcherRule strictLogStore =
      new TemporaryLogStoreAndSearcherRule(false, true);

  public KaldbQueryParserTest() throws IOException {}

  @BeforeClass
  public static void initTests() {
    Tracing.newBuilder().build();
  }

  @Test
  public void testExistsQuery() {
    // indexed=true analyzed=false - Use ReservedField
    withStringField(LogMessage.ReservedField.SERVICE_NAME.fieldName);

    // indexed=true analyzed=true - Use ReservedField
    withTextField(LogMessage.ReservedField.USERNAME.fieldName);

    // indexed=true storeDocValues=true - Use ReservedField
    withLongField(LogMessage.ReservedField.DURATION_MS.fieldName);

    // TODO: if we don't use reserved fields - Test what happens for both 1> long and 2> texty fields
    // TODO: How to structure the test to test out int fields, bool fields? Add reserved fields for them?
  }

  @Ignore
  private void withTextField(String field) {
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, "test", time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "test", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, "test");
  }

  @Ignore
  public void withStringField(String field) {
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, "test", time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "test", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, "test");
  }

  public void withLongField(String field) {
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, 100L, time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "test", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, "100");
  }

  public void canFindDocuments(Instant startTime, String field, String value) {
    SearchResult<LogMessage> result =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            field + ":" + value,
            startTime.toEpochMilli(),
            startTime.plusSeconds(1).toEpochMilli(),
            100,
            0);
    assertThat(result.hits.size()).isEqualTo(1);
    assertThat(result.totalCount).isEqualTo(1);
    assertThat(result.buckets.size()).isEqualTo(0);

    String queryStr = field + ":*";
    result =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            queryStr,
            startTime.toEpochMilli(),
            startTime.plusSeconds(1).toEpochMilli(),
            100,
            0);
    assertThat(result.hits.size()).isEqualTo(1);
    assertThat(result.totalCount).isEqualTo(1);
    assertThat(result.buckets.size()).isEqualTo(0);

    queryStr = "_exists_:" + field;
    result =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            queryStr,
            startTime.toEpochMilli(),
            startTime.plusSeconds(1).toEpochMilli(),
            100,
            0);
    assertThat(result.hits.size()).isEqualTo(1);
    assertThat(result.totalCount).isEqualTo(1);
    assertThat(result.buckets.size()).isEqualTo(0);
  }

  // used to create a LogMessage with a StringField on which we will perform exists query
  private static LogMessage makeMessageForExistsSearch(
      String indexName, String id, String stringFieldName, String fieldValue, Instant ts) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, ts.toString());
    if (stringFieldName != null) {
      fieldMap.put(stringFieldName, fieldValue);
    }
    LogWireMessage wireMsg = new LogWireMessage(indexName, TEST_MESSAGE_TYPE, id, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }

  // used to create a LogMessage with a IntField on which we will perform exists query
  private static LogMessage makeMessageForExistsSearch(
      String indexName, String id, String intFieldName, Long fieldValue, Instant ts) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, ts.toString());
    fieldMap.put(intFieldName, fieldValue);
    LogWireMessage wireMsg = new LogWireMessage(indexName, TEST_MESSAGE_TYPE, id, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }
}
