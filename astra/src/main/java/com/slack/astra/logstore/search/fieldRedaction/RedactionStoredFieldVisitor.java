package com.slack.astra.logstore.search.fieldRedaction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.FieldInfo;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

// Implements the hashing/redaction for stored fields
class RedactionStoredFieldVisitor extends StoredFieldVisitor {
    private ObjectMapper om = new ObjectMapper();
    private final StoredFieldVisitor delegate;
    private final Map<String, FieldRedactionMetadata> fieldRedactionsMap;

    // todo - listsync at field level because you want the latest metadata
    // can initiate a watcher on it and update a map

    public RedactionStoredFieldVisitor(
            final StoredFieldVisitor delegate, Map<String, FieldRedactionMetadata> fieldRedactionsMap) {
        super();
        this.delegate = delegate;
        this.fieldRedactionsMap = fieldRedactionsMap;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        return delegate.needsField(fieldInfo);
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        final byte[] redactedBytes = "REDACTED".getBytes();

        if (fieldInfo.name.equals("_source")) {
            Map<String, Object> source =
                    om.readValue(value, new TypeReference<HashMap<String, Object>>() {});

            if (source.containsKey("source")) {
                Map<String, Object> innerSource = (Map<String, Object>) source.get("source");
                long timestamp =
                        Instant.parse((String) innerSource.get("_timesinceepoch")).toEpochMilli();

                fieldRedactionsMap.forEach(
                        field -> {
                            if (field.inRedactionTimerange(timestamp)) {
                                if (innerSource.containsKey(field.getFieldName())) {
                                    innerSource.put(field.getFieldName(), redactedBytes);
                                    source.put("source", innerSource);
                                }
                            }
                        });
            }
            delegate.binaryField(fieldInfo, om.writeValueAsBytes(source));
        } else {
            delegate.binaryField(fieldInfo, value);
        }
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        if (fieldInfo.name.equals("_source")) {
            Map<String, Object> source =
                    om.readValue(value, new TypeReference<HashMap<String, Object>>() {});

            if (source.containsKey("source")) {
                Map<String, Object> innerSource = (Map<String, Object>) source.get("source");
                long timestamp =
                        Instant.parse((String) innerSource.get("_timesinceepoch")).toEpochMilli();

                fieldRedactionsMap.forEach(
                        field -> {
                            if (field.inRedactionTimerange(timestamp)) {
                                if (innerSource.containsKey(field.getFieldName())) {
                                    innerSource.put(field.getFieldName(), "REDACTED");
                                    source.put("source", innerSource);
                                }
                            }
                        });
            }

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

