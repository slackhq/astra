package com.slack.kaldb.logstore;

import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.findAllMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class FieldConflictsTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    Tracing.newBuilder().build();
  }

  @Rule
  public TemporaryLogStoreAndSearcherRule strictLogStore =
      new TemporaryLogStoreAndSearcherRule(false);

  public FieldConflictsTest() throws IOException {}

  @Test
  public void testFieldConflictingFieldTypeWithSameValue() throws InterruptedException {
    final String conflictingFieldName = "conflictingField";

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_INDEX_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                "1"));
    strictLogStore.logStore.addMessage(msg1);

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_INDEX_NAME,
            "INFO",
            "2",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                1));
    strictLogStore.logStore.addMessage(msg2);

    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    final String queryByHost = "hostname:host1-dc2.abc.com";
    await()
        .untilAsserted(
            () ->
                assertThat(
                        findAllMessages(
                                strictLogStore.logSearcher,
                                MessageUtil.TEST_INDEX_NAME,
                                queryByHost,
                                1000,
                                1)
                            .size())
                    .isEqualTo(2));

    final String conflictingTypeByNumber = conflictingFieldName + ":1";
    Collection<LogMessage> searchByInt =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_INDEX_NAME,
            conflictingTypeByNumber,
            1000,
            1);
    assertThat(searchByInt.size()).isEqualTo(2);

    final String conflictingTypeExactMatch = conflictingFieldName + ":\"1\"";
    Collection<LogMessage> searchByNumber =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_INDEX_NAME,
            conflictingTypeExactMatch,
            1000,
            1);
    assertThat(searchByNumber.size()).isEqualTo(2);
  }

  @Test
  public void testFieldConflictingFieldTypeWithDifferentValue() throws InterruptedException {
    final String conflictingFieldName = "conflictingField";

    LogMessage msg0 =
        new LogMessage(
            MessageUtil.TEST_INDEX_NAME,
            "INFO",
            "0",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                "1"));
    strictLogStore.logStore.addMessage(msg0);

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_INDEX_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                "one"));
    strictLogStore.logStore.addMessage(msg1);

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_INDEX_NAME,
            "INFO",
            "2",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                200));
    strictLogStore.logStore.addMessage(msg2);

    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    final String queryByHost = "hostname:host1-dc2.abc.com";
    await()
        .untilAsserted(
            () ->
                assertThat(
                        findAllMessages(
                                strictLogStore.logSearcher,
                                MessageUtil.TEST_INDEX_NAME,
                                queryByHost,
                                1000,
                                1)
                            .size())
                    .isEqualTo(3));

    final String conflictingTypeByString = conflictingFieldName + ":1";
    Collection<LogMessage> searchByString =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_INDEX_NAME,
            conflictingTypeByString,
            1000,
            1);
    assertThat(searchByString.size()).isEqualTo(1);

    final String conflictingTypeByExactString = conflictingFieldName + ":\"1\"";
    Collection<LogMessage> searchByExactString =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_INDEX_NAME,
            conflictingTypeByExactString,
            1000,
            1);
    assertThat(searchByExactString.size()).isEqualTo(1);

    final String conflictingTypeByString1 = conflictingFieldName + ":one";
    Collection<LogMessage> searchByString1 =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_INDEX_NAME,
            conflictingTypeByString1,
            1000,
            1);
    assertThat(searchByString1.size()).isEqualTo(1);

    final String conflictingTypeByExactString1 = conflictingFieldName + ":\"one\"";
    Collection<LogMessage> searchByExactString1 =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_INDEX_NAME,
            conflictingTypeByExactString1,
            1000,
            1);
    assertThat(searchByExactString1.size()).isEqualTo(1);

    final String conflictingTypeByNumber = conflictingFieldName + ":200";
    Collection<LogMessage> searchByNumber =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_INDEX_NAME,
            conflictingTypeByNumber,
            1000,
            1);
    assertThat(searchByNumber.size()).isEqualTo(1);

    final String conflictingTypeByNumberString = conflictingFieldName + ":\"200\"";
    Collection<LogMessage> searchByNumberString =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_INDEX_NAME,
            conflictingTypeByNumberString,
            1000,
            1);
    assertThat(searchByNumberString.size()).isEqualTo(1);
  }
}
