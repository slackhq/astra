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
    Document testDocument = docBuilder.fromMessage(MessageUtil.makeMessage(0));
    assertThat(testDocument.getFields().size()).isEqualTo(12);
  }
}
