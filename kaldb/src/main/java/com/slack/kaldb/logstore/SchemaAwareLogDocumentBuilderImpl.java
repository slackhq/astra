package com.slack.kaldb.logstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.slack.kaldb.util.JsonUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SchemaAwareLogDocumentBuilder always indexes a field using the same type. It doesn't allow field
 * conflicts.
 *
 * <p>In case of a field conflict, this class uses FieldConflictPolicy to handle them.
 *
 * <p>NOTE: Currently, if building a document raises errors, we still register the type of the
 * fields in this document partially. While the document may not be indexed, this partial field
 * config will exist in the system. For now, we assume storing this metadata is fine since it is
 * rarely an issue and helps with performance. If this is an issue, we need to scan the json twice
 * to ensure document is good to index.
 */
public class SchemaAwareLogDocumentBuilderImpl implements DocumentBuilder<LogMessage> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SchemaAwareLogDocumentBuilderImpl.class);

  public enum FieldType {
    TEXT("text") {
      @Override
      public void addField(Document doc, String name, Object value, FieldDef fieldDef) {
        addTextField(doc, name, (String) value, fieldDef);
      }
    },
    INTEGER("integer") {
      @Override
      public void addField(Document doc, String name, Object v, FieldDef fieldDef) {
        int value = (int) v;
        if (fieldDef.isIndexed) {
          doc.add(new IntPoint(name, value));
        }
        if (fieldDef.isStored) {
          doc.add(new StoredField(name, value));
        }
        if (fieldDef.storeNumericDocValue) {
          doc.add(new NumericDocValuesField(name, value));
        }
      }
    },
    LONG("long") {
      @Override
      public void addField(Document doc, String name, Object v, FieldDef fieldDef) {
        long value = (long) v;
        if (fieldDef.isIndexed) {
          doc.add(new LongPoint(name, value));
        }
        if (fieldDef.isStored) {
          doc.add(new StoredField(name, value));
        }
        if (fieldDef.storeNumericDocValue) {
          doc.add(new NumericDocValuesField(name, value));
        }
      }
    },
    FLOAT("float") {
      @Override
      public void addField(Document doc, String name, Object v, FieldDef fieldDef) {
        float value = (float) v;
        if (fieldDef.isIndexed) {
          doc.add(new FloatPoint(name, value));
        }
        if (fieldDef.isStored) {
          doc.add(new StoredField(name, value));
        }
        if (fieldDef.storeNumericDocValue) {
          doc.add(new FloatDocValuesField(name, value));
        }
      }
    },
    DOUBLE("double") {
      @Override
      public void addField(Document doc, String name, Object v, FieldDef fieldDef) {
        double value = (double) v;
        if (fieldDef.isIndexed) {
          doc.add(new DoublePoint(name, value));
        }
        if (fieldDef.isStored) {
          doc.add(new StoredField(name, value));
        }
        if (fieldDef.storeNumericDocValue) {
          doc.add(new DoubleDocValuesField(name, value));
        }
      }
    },
    BOOLEAN("boolean") {
      @Override
      public void addField(Document doc, String name, Object value, FieldDef fieldDef) {
        // Lucene has no native support for Booleans so store that field as text.
        if ((boolean) value) {
          addTextField(doc, name, "true", fieldDef);
        } else {
          addTextField(doc, name, "false", fieldDef);
        }
      }
    };

    private final String name;

    FieldType(String name) {
      this.name = name;
    }

    public abstract void addField(Document doc, String name, Object value, FieldDef fieldDef);

    public String getName() {
      return name;
    }

    static Object convertFieldValue(Object value, FieldType fromType, FieldType toType) {
      // String type
      if (fromType == FieldType.TEXT) {
        if (toType == FieldType.INTEGER) {
          return Integer.valueOf((String) value);
        }
        if (toType == FieldType.LONG) {
          return Long.valueOf((String) value);
        }
        if (toType == FieldType.FLOAT || toType == FieldType.DOUBLE) {
          return Double.valueOf((String) value);
        }
      }

      // Int type
      if (fromType == FieldType.INTEGER) {
        if (toType == FieldType.TEXT) {
          return ((Integer) value).toString();
        }
        if (toType == FieldType.LONG) {
          return ((Integer) value).longValue();
        }
        if (toType == FieldType.FLOAT) {
          return ((Integer) value).floatValue();
        }
        if (toType == FieldType.DOUBLE) {
          return ((Integer) value).doubleValue();
        }
      }

      // Long type
      if (fromType == FieldType.LONG) {
        if (toType == FieldType.TEXT) {
          return ((Long) value).toString();
        }
        if (toType == FieldType.INTEGER) {
          return ((Long) value).intValue();
        }
        if (toType == FieldType.FLOAT) {
          return ((Long) value).floatValue();
        }
        if (toType == FieldType.DOUBLE) {
          return ((Long) value).doubleValue();
        }
      }

      // Float type
      if (fromType == FieldType.FLOAT) {
        if (toType == FieldType.TEXT) {
          return value.toString();
        }
        if (toType == FieldType.INTEGER) {
          return ((Float) value).intValue();
        }
        if (toType == FieldType.LONG) {
          return ((Float) value).longValue();
        }
        if (toType == FieldType.DOUBLE) {
          return ((Float) value).doubleValue();
        }
      }

      // Double type
      if (fromType == FieldType.DOUBLE) {
        if (toType == FieldType.TEXT) {
          return value.toString();
        }
        if (toType == FieldType.INTEGER) {
          return ((Double) value).intValue();
        }
        if (toType == FieldType.LONG) {
          return ((Double) value).longValue();
        }
        if (toType == FieldType.FLOAT) {
          return ((Double) value).floatValue();
        }
      }

      return null;
    }
  }

  /*
   * FieldDef describes the configs that can be set on a field.
   */
  static class FieldDef {
    public final FieldType fieldType;
    public final boolean isStored;
    public final boolean isIndexed;
    public final boolean isAnalyzed;
    public final boolean storeNumericDocValue;

    FieldDef(FieldType fieldType, boolean isStored, boolean isIndexed, boolean isAnalyzed) {
      this(fieldType, isStored, isIndexed, isAnalyzed, false);
    }

    FieldDef(
        FieldType fieldType,
        boolean isStored,
        boolean isIndexed,
        boolean isAnalyzed,
        boolean storeNumericDocValue) {
      if (isAnalyzed && !isIndexed) {
        throw new InvalidFieldDefException("Cannot set isAnalyzed without setting isIndexed");
      }

      if (isAnalyzed && !(fieldType.equals(FieldType.TEXT))) {
        throw new InvalidFieldDefException("Only text and any types can have isAnalyzed set");
      }

      this.fieldType = fieldType;
      this.isStored = isStored;
      this.isIndexed = isIndexed;
      this.isAnalyzed = isAnalyzed;
      this.storeNumericDocValue = storeNumericDocValue;
    }
  }

  private static ImmutableMap<String, FieldDef> getDefaultFieldDefinitions() {
    ImmutableMap.Builder<String, FieldDef> defaultFieldDefMapBuilder = ImmutableMap.builder();
    defaultFieldDefMapBuilder.put(
        LogMessage.SystemField.SOURCE.fieldName, new FieldDef(FieldType.TEXT, true, false, false));
    defaultFieldDefMapBuilder.put(
        LogMessage.SystemField.ID.fieldName, new FieldDef(FieldType.TEXT, false, true, true));
    defaultFieldDefMapBuilder.put(
        LogMessage.SystemField.INDEX.fieldName, new FieldDef(FieldType.TEXT, false, true, true));
    defaultFieldDefMapBuilder.put(
        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        new FieldDef(FieldType.LONG, false, true, false, true));
    defaultFieldDefMapBuilder.put(
        LogMessage.SystemField.TYPE.fieldName, new FieldDef(FieldType.TEXT, false, true, true));

    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.HOSTNAME.fieldName,
        new FieldDef(FieldType.TEXT, false, true, true));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.PACKAGE.fieldName,
        new FieldDef(FieldType.TEXT, false, true, true));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.MESSAGE.fieldName,
        new FieldDef(FieldType.TEXT, false, true, true));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.TAG.fieldName, new FieldDef(FieldType.TEXT, false, true, true));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.TIMESTAMP.fieldName,
        new FieldDef(FieldType.TEXT, false, true, true));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.USERNAME.fieldName,
        new FieldDef(FieldType.TEXT, false, true, true));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.PAYLOAD.fieldName,
        new FieldDef(FieldType.TEXT, false, false, false));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.NAME.fieldName, new FieldDef(FieldType.TEXT, false, true, false));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.SERVICE_NAME.fieldName,
        new FieldDef(FieldType.TEXT, false, true, false));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.DURATION_MS.fieldName,
        new FieldDef(FieldType.LONG, false, true, false));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.TRACE_ID.fieldName,
        new FieldDef(FieldType.TEXT, false, true, false));
    defaultFieldDefMapBuilder.put(
        LogMessage.ReservedField.PARENT_ID.fieldName,
        new FieldDef(FieldType.TEXT, false, true, false));

    return defaultFieldDefMapBuilder.build();
  }

  /**
   * This enum tracks the field conflict policy for a chunk.
   *
   * <p>NOTE: In future, we may need this granularity at a per field level. Also, other potential
   * options for handling these conflicts: (a) store all the conflicted fields as strings by default
   * so querying those fields is more consistent. (b) duplicate field value only but don't create a
   * field.
   */
  public enum FieldConflictPolicy {
    // Throw an error on field conflict.
    RAISE_ERROR,
    // Drop the conflicting field.
    DROP_FIELD,
    // Convert the field value to the type of the conflicting field.
    CONVERT_FIELD_VALUE,
    // Covert the field value to the type of conflicting field and also create a new field of type.
    CONVERT_AND_DUPLICATE_FIELD
  }

  private static final Map<FieldType, FieldDef> defaultPropDescriptionForType =
      ImmutableMap.of(
          FieldType.LONG,
          new FieldDef(FieldType.LONG, true, false, false, true),
          FieldType.FLOAT,
          new FieldDef(FieldType.FLOAT, true, false, false, true),
          FieldType.INTEGER,
          new FieldDef(FieldType.INTEGER, true, false, false, true),
          FieldType.DOUBLE,
          new FieldDef(FieldType.DOUBLE, true, false, false, true),
          FieldType.TEXT,
          new FieldDef(FieldType.TEXT, false, true, true),
          FieldType.BOOLEAN,
          new FieldDef(FieldType.BOOLEAN, false, true, false));

  @VisibleForTesting
  public FieldConflictPolicy getIndexFieldConflictPolicy() {
    return indexFieldConflictPolicy;
  }

  public Map<String, FieldDef> getFieldDefMap() {
    return fieldDefMap;
  }

  private void addField(
      final Document doc, final String key, final Object value, final String keyPrefix) {
    // If value is a list, convert the value to a String and index the field.
    if (value instanceof List) {
      addField(doc, key, Strings.join((List) value, ','), keyPrefix);
      return;
    }

    String fieldName = keyPrefix.isBlank() || keyPrefix.isEmpty() ? key : keyPrefix + "." + key;
    // TODO: Add a depth limit for recursion.
    // Ingest nested map field recursively.
    if (value instanceof Map) {
      Map<Object, Object> mapValue = (Map<Object, Object>) value;
      for (Object k : mapValue.keySet()) {
        if (k instanceof String) {
          addField(doc, (String) k, mapValue.get(k), fieldName);
        } else {
          throw new FieldDefMismatchException(
              String.format("Field %s, %s has an non-string type which is unsupported", k, value));
        }
      }
      return;
    }

    FieldType valueType = getJsonType(value);
    if (!fieldDefMap.containsKey(fieldName)) {
      indexNewField(doc, fieldName, value, valueType);
    } else {
      FieldDef registeredField = fieldDefMap.get(fieldName);
      if (registeredField.fieldType == valueType) {
        // No field conflicts index it using previous description.
        indexTypedField(doc, fieldName, value, registeredField);
      } else {
        // There is a field type conflict, index it using the field conflict policy.
        switch (indexFieldConflictPolicy) {
          case DROP_FIELD:
            LOG.debug("Dropped field {} due to field type conflict", fieldName);
            droppedFieldsCounter.increment();
            break;
          case CONVERT_FIELD_VALUE:
            convertValueAndIndexField(value, valueType, registeredField, doc, fieldName);
            LOG.debug(
                "Converting field {} value from type {} to {} due to type conflict",
                fieldName,
                valueType,
                registeredField.fieldType);
            convertFieldValueCounter.increment();
            break;
          case CONVERT_AND_DUPLICATE_FIELD:
            convertValueAndIndexField(value, valueType, registeredField, doc, fieldName);
            LOG.debug(
                "Converting field {} value from type {} to {} due to type conflict",
                fieldName,
                valueType,
                registeredField.fieldType);
            // Add new field with new type
            String newFieldName = makeNewFieldOfType(fieldName, valueType);
            indexNewField(doc, newFieldName, value, valueType);
            LOG.debug(
                "Added new field {} of type {} due to type conflict", newFieldName, valueType);
            convertAndDuplicateFieldCounter.increment();
            break;
          case RAISE_ERROR:
            throw new FieldDefMismatchException(
                String.format(
                    "Field type for field %s is %s but new value is of type  %s. ",
                    fieldName, registeredField.fieldType, valueType));
        }
      }
    }
  }

  private void indexNewField(Document doc, String key, Object value, FieldType valueType) {
    // If we are seeing a field for the first time index it with default template for the
    // valueType and create a field def.
    if (!defaultPropDescriptionForType.containsKey(valueType)) {
      throw new RuntimeException("No default prop description");
    }

    FieldDef defaultPropDescription = defaultPropDescriptionForType.get(valueType);
    // add the document to this field.
    fieldDefMap.put(key, defaultPropDescription);
    indexTypedField(doc, key, value, defaultPropDescription);
  }

  static String makeNewFieldOfType(String key, FieldType valueType) {
    return key + "_" + valueType.getName();
  }

  private static void convertValueAndIndexField(
      Object value, FieldType valueType, FieldDef registeredField, Document doc, String key) {
    Object convertedValue =
        FieldType.convertFieldValue(value, valueType, registeredField.fieldType);
    if (convertedValue == null) {
      throw new RuntimeException("No mapping found to convert value");
    }
    indexTypedField(doc, key, convertedValue, registeredField);
  }

  private static void indexTypedField(Document doc, String key, Object value, FieldDef fieldDef) {
    fieldDef.fieldType.addField(doc, key, value, fieldDef);
    // TODO: Ignore exceptional fields when needed?
  }

  private static void addTextField(Document doc, String name, String value, FieldDef description) {
    if (description.isIndexed) {
      if (description.isAnalyzed) {
        doc.add(new TextField(name, value, getStoreEnum(description.isStored)));
      } else {
        doc.add(new StringField(name, value, getStoreEnum(description.isStored)));
      }
    } else {
      if (description.isStored) {
        doc.add(new StoredField(name, value));
      }
    }
  }

  private static Field.Store getStoreEnum(boolean isStored) {
    return isStored ? Field.Store.YES : Field.Store.NO;
  }

  private static FieldType getJsonType(Object value) {
    if (value instanceof Long) {
      return FieldType.LONG;
    }
    if (value instanceof Integer) {
      return FieldType.INTEGER;
    }
    if (value instanceof String) {
      return FieldType.TEXT;
    }
    if (value instanceof Float) {
      return FieldType.FLOAT;
    }
    if (value instanceof Boolean) {
      return FieldType.BOOLEAN;
    }
    if (value instanceof Double) {
      return FieldType.DOUBLE;
    }

    // TODO: Handle other tyoes like map and list or nested objects?
    throw new RuntimeException("Unknown type");
  }

  public static SchemaAwareLogDocumentBuilderImpl build(
      FieldConflictPolicy fieldConflictPolicy, MeterRegistry meterRegistry) {
    // Add basic fields by default
    return new SchemaAwareLogDocumentBuilderImpl(
        fieldConflictPolicy, getDefaultFieldDefinitions(), meterRegistry);
  }

  static final String DROP_FIELDS_COUNTER = "dropped_fields";
  static final String CONVERT_FIELD_VALUE_COUNTER = "convert_field_value";
  static final String CONVERT_AND_DUPLICATE_FIELD_COUNTER = "convert_and_duplicate_field";

  private final FieldConflictPolicy indexFieldConflictPolicy;
  private final Map<String, FieldDef> fieldDefMap = new ConcurrentHashMap<>();
  private final Counter droppedFieldsCounter;
  private final Counter convertFieldValueCounter;
  private final Counter convertAndDuplicateFieldCounter;

  SchemaAwareLogDocumentBuilderImpl(
      FieldConflictPolicy indexFieldConflictPolicy,
      final Map<String, FieldDef> initialFields,
      MeterRegistry meterRegistry) {
    this.indexFieldConflictPolicy = indexFieldConflictPolicy;
    fieldDefMap.putAll(initialFields);
    // Note: Consider adding field name as a tag to help debugging, but it's high cardinality.
    droppedFieldsCounter = meterRegistry.counter(DROP_FIELDS_COUNTER);
    convertFieldValueCounter = meterRegistry.counter(CONVERT_FIELD_VALUE_COUNTER);
    convertAndDuplicateFieldCounter = meterRegistry.counter(CONVERT_AND_DUPLICATE_FIELD_COUNTER);
  }

  @Override
  public Document fromMessage(LogMessage message) throws JsonProcessingException {
    Document doc = new Document();
    addField(doc, LogMessage.SystemField.INDEX.fieldName, message.getIndex(), "");
    addField(
        doc, LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, message.timeSinceEpochMilli, "");
    addField(doc, LogMessage.SystemField.TYPE.fieldName, message.getType(), "");
    addField(doc, LogMessage.SystemField.ID.fieldName, message.id, "");
    addField(
        doc,
        LogMessage.SystemField.SOURCE.fieldName,
        JsonUtil.writeAsString(message.toWireMessage()),
        "");
    for (String key : message.source.keySet()) {
      LOG.info("Adding key {}", key);
      addField(doc, key, message.source.get(key), "");
    }
    return doc;
  }
}
