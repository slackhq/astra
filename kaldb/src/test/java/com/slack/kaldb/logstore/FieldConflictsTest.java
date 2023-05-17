package com.slack.kaldb.logstore;

import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.findAllMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class FieldConflictsTest {
  @BeforeAll
  public static void beforeClass() {
    Tracing.newBuilder().build();
  }

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension strictLogStore =
      new TemporaryLogStoreAndSearcherExtension(true);

  public FieldConflictsTest() throws IOException {}

  @Test
  public void testFieldConflictingFieldTypeWithSameValue() {
    final String conflictingFieldName = "conflictingField";

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Instant.now(),
            Map.of(
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
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "2",
            Instant.now(),
            Map.of(
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
                                MessageUtil.TEST_DATASET_NAME,
                                queryByHost,
                                1000)
                            .size())
                    .isEqualTo(2));

    final String conflictingTypeByNumber = conflictingFieldName + ":1";
    Collection<LogMessage> searchByInt =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_DATASET_NAME,
            conflictingTypeByNumber,
            1000);
    assertThat(searchByInt.size()).isEqualTo(2);

    final String conflictingTypeExactMatch = conflictingFieldName + ":\"1\"";
    Collection<LogMessage> searchByNumber =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_DATASET_NAME,
            conflictingTypeExactMatch,
            1000);
    assertThat(searchByNumber.size()).isEqualTo(2);
  }

  @Test
  public void testFieldConflictingFieldTypeWithDifferentValue() {
    final String conflictingFieldName = "conflictingField";

    LogMessage msg0 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "0",
            Instant.now(),
            Map.of(
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
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Instant.now(),
            Map.of(
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
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "2",
            Instant.now(),
            Map.of(
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
                                MessageUtil.TEST_DATASET_NAME,
                                queryByHost,
                                1000)
                            .size())
                    .isEqualTo(3));

    final String conflictingTypeByString = conflictingFieldName + ":1";
    Collection<LogMessage> searchByString =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_DATASET_NAME,
            conflictingTypeByString,
            1000);
    assertThat(searchByString.size()).isEqualTo(1);

    final String conflictingTypeByExactString = conflictingFieldName + ":\"1\"";
    Collection<LogMessage> searchByExactString =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_DATASET_NAME,
            conflictingTypeByExactString,
            1000);
    assertThat(searchByExactString.size()).isEqualTo(1);

    final String conflictingTypeByString1 = conflictingFieldName + ":one";
    Collection<LogMessage> searchByString1 =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_DATASET_NAME,
            conflictingTypeByString1,
            1000);
    assertThat(searchByString1.size()).isEqualTo(1);

    final String conflictingTypeByExactString1 = conflictingFieldName + ":\"one\"";
    Collection<LogMessage> searchByExactString1 =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_DATASET_NAME,
            conflictingTypeByExactString1,
            1000);
    assertThat(searchByExactString1.size()).isEqualTo(1);

    final String conflictingTypeByNumber = conflictingFieldName + ":200";
    Collection<LogMessage> searchByNumber =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_DATASET_NAME,
            conflictingTypeByNumber,
            1000);
    assertThat(searchByNumber.size()).isEqualTo(1);

    final String conflictingTypeByNumberString = conflictingFieldName + ":\"200\"";
    Collection<LogMessage> searchByNumberString =
        findAllMessages(
            strictLogStore.logSearcher,
            MessageUtil.TEST_DATASET_NAME,
            conflictingTypeByNumberString,
            1000);
    assertThat(searchByNumberString.size()).isEqualTo(1);
  }
}
