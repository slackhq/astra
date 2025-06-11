package com.slack.astra.logstore.search.fieldRedaction;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

/**
 * RedactionStoreFieldVisitor reads the individual fields of the logs that come back from a search
 * and redacts the values if necessary using the values in FieldRedactionMetadataStore.
 */
class RedactionStoredFieldVisitor extends StoredFieldVisitor {
  private ObjectMapper om = new ObjectMapper();
  private final StoredFieldVisitor delegate;
  private final Map<String, FieldRedactionMetadata> fieldRedactionsMap;
  private final String redactedValue = "REDACTED";

  public RedactionStoredFieldVisitor(
      final StoredFieldVisitor delegate,
      HashMap<String, FieldRedactionMetadata> fieldRedactionsMap) {
    super();
    this.delegate = delegate;
    this.fieldRedactionsMap = fieldRedactionsMap;
  }

  private Map<String, Object> redactField(byte[] value) throws IOException {
    Map<String, Object> source =
        om.readValue(value, new TypeReference<HashMap<String, Object>>() {});

    if (source.containsKey("source")) {
      Map<String, Object> innerSource = (Map<String, Object>) source.get("source");
      long timestamp =
          Instant.parse((String) innerSource.get(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName))
              .toEpochMilli();

      for (FieldRedactionMetadata fieldRedactionMetadata : fieldRedactionsMap.values()) {
        if (fieldRedactionMetadata.inRedactionTimerange(timestamp)) {
          if (innerSource.containsKey(fieldRedactionMetadata.getFieldName())) {
            innerSource.put(fieldRedactionMetadata.getFieldName(), redactedValue);
            source.put("source", innerSource);
          }
        }
      }
    }
    return source;
  }

  @Override
  public Status needsField(FieldInfo fieldInfo) throws IOException {
    return delegate.needsField(fieldInfo);
  }

  @Override
  public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {

    if (fieldInfo.name.equals("_source")) {
      Map<String, Object> source = redactField(value);
      delegate.binaryField(fieldInfo, om.writeValueAsBytes(source));
    } else {
      delegate.binaryField(fieldInfo, value);
    }
  }

  @Override
  public void stringField(FieldInfo fieldInfo, String value) throws IOException {
    if (fieldInfo.name.equals("_source")) {
      Map<String, Object> source = redactField(value.getBytes(UTF_8));
      delegate.stringField(fieldInfo, om.writeValueAsString(source));
    } else {
      delegate.stringField(fieldInfo, value);
    }
  }

  @Override
  public void intField(FieldInfo fieldInfo, int value) throws IOException {
    delegate.intField(fieldInfo, value);
  }

  @Override
  public void longField(FieldInfo fieldInfo, long value) throws IOException {
    delegate.longField(fieldInfo, value);
  }

  @Override
  public void floatField(FieldInfo fieldInfo, float value) throws IOException {
    delegate.floatField(fieldInfo, value);
  }

  @Override
  public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
    delegate.doubleField(fieldInfo, value);
  }
}
