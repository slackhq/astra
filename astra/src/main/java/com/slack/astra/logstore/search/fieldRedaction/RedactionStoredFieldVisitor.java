package com.slack.astra.logstore.search.fieldRedaction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

class RedactionStoredFieldVisitor extends StoredFieldVisitor {
  private ObjectMapper om = new ObjectMapper();
  private final StoredFieldVisitor delegate;
  private final Map<String, FieldRedactionMetadata> fieldRedactionsMap;
  private final String redactedValue = "REDACTED";

  public RedactionStoredFieldVisitor(
      final StoredFieldVisitor delegate, FieldRedactionMetadataStore fieldRedactionMetadataStore) {
    super();
    this.delegate = delegate;

    Map<String, FieldRedactionMetadata> fieldRedactionsMap = new HashMap<>();
    fieldRedactionMetadataStore
        .listSync()
        .forEach(
            redaction -> {
              fieldRedactionsMap.put(redaction.getName(), redaction);
            });

    // TODO - do we need the listener at all if we listsync at the field level anyway?
    AstraMetadataStoreChangeListener<FieldRedactionMetadata> listener =
        new AstraMetadataStoreChangeListener() {
          @Override
          public void onMetadataStoreChanged(Object model) {
            fieldRedactionMetadataStore
                .listSync()
                .forEach(
                    redaction -> {
                      fieldRedactionsMap.put(redaction.getName(), redaction);
                    });
          }
        };
    fieldRedactionMetadataStore.addListener(listener);
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
      Map<String, Object> source = redactField(value.getBytes());
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
