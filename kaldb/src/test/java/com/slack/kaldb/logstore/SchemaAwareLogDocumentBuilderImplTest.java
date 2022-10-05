package com.slack.kaldb.logstore;

import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.testlib.MessageUtil;
import java.io.IOException;
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
    final LogMessage message = MessageUtil.makeMessage(0);
    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(12);
  }

  // TODO: Add a unit test for messages with field conflicts and handling
  // TODO: Add every type to every type change.
}
