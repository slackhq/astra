package com.slack.kaldb.logstore.search.queryparser;

import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MessageUtil.TEST_MESSAGE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class KaldbQueryParserTest {
  @Rule
  public TemporaryLogStoreAndSearcherRule strictLogStore =
      new TemporaryLogStoreAndSearcherRule(false);

  public KaldbQueryParserTest() throws IOException {}

  @BeforeClass
  public static void initTests() {
    Tracing.newBuilder().build();
  }

  @Test
  public void testInit() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new KaldbQueryParser("test", new StandardAnalyzer(), null));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new KaldbQueryParser("test", new StandardAnalyzer(), new ConcurrentHashMap<>()));
  }

  @Test
  public void testWithStringField() {
    // indexed=true analyzed=false storeDocValues=true - Use ReservedField
    String field = LogMessage.ReservedField.SERVICE_NAME.fieldName;
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, "test", time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "test", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    canFindDocuments(time, field, "test");

    // indexed=true analyzed=false storeDocValues=true - Not ReservedField
    field = "my_service_name";
    time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, "test", time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "test", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    canFindDocuments(time, field, "test");
  }

  @Test
  public void testWithTextField() {
    // indexed=true analyzed=true - Use ReservedField
    // All texty fields that are not reserved will use analyzed=false aka string field which we
    // already tested
    String field = LogMessage.ReservedField.USERNAME.fieldName;
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, "test", time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "test", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, "test");
  }

  @Test
  public void testWithLongField() {
    // indexed=true storeDocValues=true - Use ReservedField
    String field = LogMessage.ReservedField.DURATION_MS.fieldName;
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, Long.MAX_VALUE, time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "empty", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, String.valueOf(Long.MAX_VALUE));

    // indexed=true storeDocValues=true - Not ReservedField
    field = "my_duration_ms";
    time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, Long.MAX_VALUE, time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "empty", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, String.valueOf(Long.MAX_VALUE));
  }

  @Test
  public void testWithIntField() {
    String field = "my_int_field";
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, Integer.MAX_VALUE, time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "empty", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, String.valueOf(Integer.MAX_VALUE));
  }

  @Test
  public void testWithFloatField() {
    String field = "my_float_field";
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, Float.MAX_VALUE, time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "empty", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, String.valueOf(Float.MAX_VALUE));
  }

  @Test
  public void testWithDoubleField() {
    String field = "my_double_field";
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, Double.MIN_VALUE, time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "empty", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, String.valueOf(Double.MIN_VALUE));
  }

  @Test
  @Ignore // disabled until true boolean query support is provided
  public void testWithBooleanField() {
    String field = "my_boolean_field";
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "1", field, Boolean.TRUE, time));
    strictLogStore.logStore.addMessage(
        makeMessageForExistsSearch("testIndex", "2", null, "empty", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    canFindDocuments(time, field, String.valueOf(Boolean.TRUE));
  }

  public void canFindDocuments(Instant startTime, String field, String value) {
    SearchResult<LogMessage> result =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            field + ":" + value,
            startTime.toEpochMilli(),
            startTime.plusSeconds(1).toEpochMilli(),
            100,
            null);
    assertThat(result.hits.size()).isEqualTo(1);
    assertThat(result.internalAggregation).isEqualTo(null);

    String queryStr = field + ":*";
    result =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            queryStr,
            startTime.toEpochMilli(),
            startTime.plusSeconds(1).toEpochMilli(),
            100,
            null);
    assertThat(result.hits.size()).isEqualTo(1);
    assertThat(result.internalAggregation).isEqualTo(null);

    queryStr = "_exists_:" + field;
    result =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            queryStr,
            startTime.toEpochMilli(),
            startTime.plusSeconds(1).toEpochMilli(),
            100,
            null);
    assertThat(result.hits.size()).isEqualTo(1);
    assertThat(result.internalAggregation).isEqualTo(null);
  }

  // used to create a LogMessage with a StringField on which we will perform exists query
  private static LogMessage makeMessageForExistsSearch(
      String indexName, String id, String stringFieldName, String fieldValue, Instant ts) {
    Map<String, Object> fieldMap = new HashMap<>();
    if (stringFieldName != null) {
      fieldMap.put(stringFieldName, fieldValue);
    }
    LogWireMessage wireMsg = new LogWireMessage(indexName, TEST_MESSAGE_TYPE, id, ts, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }

  // used to create a LogMessage with a LongField on which we will perform exists query
  private static LogMessage makeMessageForExistsSearch(
      String indexName, String id, String intFieldName, Long fieldValue, Instant ts) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(intFieldName, fieldValue);
    LogWireMessage wireMsg = new LogWireMessage(indexName, TEST_MESSAGE_TYPE, id, ts, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }

  // used to create a LogMessage with a IntegerField on which we will perform exists query
  private static LogMessage makeMessageForExistsSearch(
      String indexName, String id, String intFieldName, Integer fieldValue, Instant ts) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(intFieldName, fieldValue);
    LogWireMessage wireMsg = new LogWireMessage(indexName, TEST_MESSAGE_TYPE, id, ts, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }

  // used to create a LogMessage with a IntegerField on which we will perform exists query
  private static LogMessage makeMessageForExistsSearch(
      String indexName, String id, String intFieldName, Float fieldValue, Instant ts) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(intFieldName, fieldValue);
    LogWireMessage wireMsg = new LogWireMessage(indexName, TEST_MESSAGE_TYPE, id, ts, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }

  // used to create a LogMessage with a IntegerField on which we will perform exists query
  private static LogMessage makeMessageForExistsSearch(
      String indexName, String id, String intFieldName, Double fieldValue, Instant ts) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(intFieldName, fieldValue);
    LogWireMessage wireMsg = new LogWireMessage(indexName, TEST_MESSAGE_TYPE, id, ts, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }

  // used to create a LogMessage with a IntegerField on which we will perform exists query
  private static LogMessage makeMessageForExistsSearch(
      String indexName, String id, String intFieldName, Boolean fieldValue, Instant ts) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(intFieldName, fieldValue);
    LogWireMessage wireMsg = new LogWireMessage(indexName, TEST_MESSAGE_TYPE, id, ts, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }
}
