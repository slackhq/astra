package com.slack.kaldb.logstore;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.document.Document;

/**
 * SchemaAwareLogDocumentBuilder always indexes a field using the same type. It doesn't allow field
 * conflicts.
 *
 * <p>In case of a field conflict, this class uses FieldConflictPolicy to handle them.
 */
class SchemaAwareLogDocumentBuilderImpl implements DocumentBuilder<LogMessage> {

  static class FieldDef {
    public final String type;
    public final LogDocumentBuilderImpl.PropertyDescription propertyDescription;
    // No per field, field conflict policy for now. But placeholder for future.
    // public final FieldConflictPolicy fieldConflictPolicy = null;

    FieldDef(String type, LogDocumentBuilderImpl.PropertyDescription propertyDescription) {
      this.type = type;
      this.propertyDescription = propertyDescription;
    }
  }

  // TODO: Should this be a per field policy? For now, may be keep it index level?
  private enum FieldConflictPolicy {
    DROP_FIELD,
    CONVERT_FIELD_VALUE,
    CONVERT_AND_DUPLICATE_FIELD
  }

  private enum PropertyType {
    TEXT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    ANY
  }

  // TODO: Add other types. Make immutable.
  private static final Map<String, LogDocumentBuilderImpl.PropertyDescription>
      defaultPropDescriptionForType =
          Map.of(
              "Long",
              new LogDocumentBuilderImpl.PropertyDescription(
                  LogDocumentBuilderImpl.PropertyType.LONG, false, true, false),
              "String",
              new LogDocumentBuilderImpl.PropertyDescription(
                  LogDocumentBuilderImpl.PropertyType.TEXT, false, true, true));

  private final FieldConflictPolicy indexFieldConflictPolicy;
  private final Map<String, FieldDef> fieldDefMap = new ConcurrentHashMap<>();

  SchemaAwareLogDocumentBuilderImpl(FieldConflictPolicy indexFieldConflictPolicy) {
    this.indexFieldConflictPolicy = indexFieldConflictPolicy;
  }

  @Override
  public Document fromMessage(LogMessage message) throws IOException {
    Document doc = new Document();
    for (String key : message.source.keySet()) {
      addProperty(doc, key, message.source.get(key));
    }
    return doc;
  }

  private void addProperty(Document doc, String key, Object value) {
    String valueType = getJsonType(value);
    if (!fieldDefMap.containsKey(key)) {
      addFieldAndIndexField(doc, key, value, valueType);
    } else {
      FieldDef registeredField = fieldDefMap.get(key);
      if (registeredField.type == valueType) {
        // No field conflicts index it using previous description.
        indexFieldWithType(doc, key, value, registeredField.propertyDescription);
      } else {
        // There is a field type conflict, index it using the field conflict policy.
        if (indexFieldConflictPolicy == FieldConflictPolicy.DROP_FIELD) {
          return;
        }
        if (indexFieldConflictPolicy == FieldConflictPolicy.CONVERT_FIELD_VALUE) {
          convertValueAndIndexField(value, valueType, registeredField, doc, key);
        }
        if (indexFieldConflictPolicy == FieldConflictPolicy.CONVERT_AND_DUPLICATE_FIELD) {
          convertValueAndIndexField(value, valueType, registeredField, doc, key);
          // Add new field with new type
          String newFieldName = makeNewFieldOfType(key, valueType);
          addFieldAndIndexField(doc, newFieldName, value, valueType);
        }
      }
    }
  }

  private void addFieldAndIndexField(Document doc, String key, Object value, String valueType) {
    // If we are seeing a field for the first time index it with default template for the
    // valueType and create a field def.
    if (!defaultPropDescriptionForType.containsKey(valueType)) {
      throw new RuntimeException("No default prop description");
    }

    LogDocumentBuilderImpl.PropertyDescription defaultPropDescription =
        defaultPropDescriptionForType.get(valueType);
    // add the document to this field.
    fieldDefMap.put(key, new FieldDef(valueType, defaultPropDescription));
    indexFieldWithType(doc, key, value, defaultPropDescription);
  }

  private String makeNewFieldOfType(String key, String valueType) {
    return key + "_" + valueType;
  }

  private void convertValueAndIndexField(
      Object value, String valueType, FieldDef registeredField, Document doc, String key) {
    Object convertedValue = convertFieldValue(value, valueType, registeredField.type);
    if (convertedValue == null) {
      throw new RuntimeException("No mapping found to convert value");
    }
    indexFieldWithType(doc, key, convertedValue, registeredField.propertyDescription);
  }

  private Object convertFieldValue(Object value, String fromType, String toType) {
    // String type
    if (fromType.equals("String")) {
      if (toType.equals("Int")) {
        return Integer.valueOf((String) value);
      }
      if (toType.equals("Long")) {
        return Long.valueOf((String) value);
      }
      if (toType.equals("Float") || toType.equals("Double")) {
        return Double.valueOf((String) value);
      }
    }

    // Int type
    if (fromType.equals("Int")) {
      if (toType.equals("String")) {
        return ((Integer) value).toString();
      }
      if (toType.equals("Long")) {
        return ((Integer) value).longValue();
      }
      if (toType.equals("Float")) {
        return ((Integer) value).floatValue();
      }
      if (toType.equals("Float")) {
        return ((Integer) value).doubleValue();
      }
    }
    return null;
  }

  private void indexFieldWithType(
      Document doc,
      String key,
      Object value,
      LogDocumentBuilderImpl.PropertyDescription defaultPropDescription) {}

  // TODO: Return enum or java class.
  private String getJsonType(Object value) {
    if (value instanceof Long) {
      return "Long";
    }
    if (value instanceof Integer) {
      return "Int";
    }
    if (value instanceof String) {
      return "String";
    }
    if (value instanceof Float) {
      return "Float";
    }
    if (value instanceof Boolean) {
      return "Boolean";
    }
    if (value instanceof Double) {
      return "Double";
    }
    // Fix this.
    throw new RuntimeException("Unknown type");
  }
}
