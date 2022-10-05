package com.slack.kaldb.logstore;

import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.kaldb.testlib.MessageUtil;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.junit.Before;
import org.junit.Test;

public class SchemaAwareLogDocumentBuilderImplTest {

  private SchemaAwareLogDocumentBuilderImpl docBuilder;
  private LogMessage testMessage;

  @Before
  public void setup() throws IOException {
    docBuilder = new SchemaAwareLogDocumentBuilderImpl(DROP_FIELD);
    testMessage = MessageUtil.makeMessage(0);
  }

  @Test
  public void testBasicDocumentCreation() throws IOException {
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(17);
    final LogMessage message = MessageUtil.makeMessage(0);
    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(16);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(21);
    assertThat(docBuilder.getFieldDefMap().keySet())
        .containsAll(
            List.of(
                "longproperty",
                "floatproperty",
                "@timestamp",
                "intproperty",
                "message",
                "doubleproperty"));
  }

  @Test
  public void testDroppingConflictingField() throws JsonProcessingException {
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(17);
    String conflictingFieldName = "conflictingField";

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
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

    Document msg1Doc = docBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(11);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(18);
    assertThat(docBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.TEXT);

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
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
    Document msg2Doc = docBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(10);
    // Conflicting field is dropped.
    assertThat(
            msg2Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isEmpty();
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(18);
    assertThat(docBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.TEXT);
  }

  // TODO: Add a unit test for messages with field conflicts and handling
  // TODO: Add every type to every type change.
}
