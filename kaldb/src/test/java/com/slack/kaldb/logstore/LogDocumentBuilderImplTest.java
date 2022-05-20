package com.slack.kaldb.logstore;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.testlib.MessageUtil;
import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.document.Document;
import org.junit.Before;
import org.junit.Test;

public class LogDocumentBuilderImplTest {

  private DocumentBuilder<LogMessage> testBuilderAllowExceptions;
  private DocumentBuilder<LogMessage> testBuilderIgnoreExceptions;
  private LogMessage testMessage;

  @Before
  public void setup() throws IOException {
    testBuilderAllowExceptions = LogDocumentBuilderImpl.build(false);
    testBuilderIgnoreExceptions = LogDocumentBuilderImpl.build(true);
    testMessage = MessageUtil.makeMessage(0);
  }

  @Test
  public void testWithValidMessage() throws IOException {
    Document testDocument = testBuilderAllowExceptions.fromMessage(MessageUtil.makeMessage(0));
    assertThat(testDocument.getFields().size()).isEqualTo(12);
  }

  // TODO: Test IOException and JSONSerialization exception.
  @Test(expected = PropertyTypeMismatchException.class)
  public void testPropertyTypeMismatchFailure() throws IOException {
    addMismatchedPropertyType(testBuilderAllowExceptions);
  }

  @Test
  public void testSuppressPropertyMismatchTypeFailure() throws IOException {
    addMismatchedPropertyType(testBuilderIgnoreExceptions);
  }

  private void addMismatchedPropertyType(DocumentBuilder<LogMessage> builder) throws IOException {
    testMessage.addProperty("username", 0);
    builder.fromMessage(testMessage);
  }

  @Test(expected = UnSupportedPropertyTypeException.class)
  public void testUnsupportedPropertyTypeFailure() throws IOException {
    addUnsupportedPropertyType(testBuilderAllowExceptions, "badproperty");
  }

  @Test
  public void testSuppressUnsupportedPropertyFailure() throws IOException {
    addUnsupportedPropertyType(testBuilderIgnoreExceptions, "badproperty");
    addUnsupportedPropertyType(
        testBuilderIgnoreExceptions, "username"); // reserved field with unkown type.
  }

  private void addUnsupportedPropertyType(DocumentBuilder<LogMessage> builder, String key)
      throws IOException {
    testMessage.addProperty(key, Collections.EMPTY_LIST);
    builder.fromMessage(testMessage);
  }
}
