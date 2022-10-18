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

  // TODO: Add abstract methods to enum to structure fields better.
  // TODO: Add a string field name which is a string.
  public enum PropertyType {
    TEXT("text") {
      @Override
      public void addField(
          Document doc, String name, Object value, PropertyDescription propertyDescription) {
        addStringProperty(doc, name, (String) value, propertyDescription);
      }
    },

    INTEGER("integer") {
      @Override
      public void addField(
          Document doc, String name, Object v, PropertyDescription propertyDescription) {
        int value = (int) v;
        if (propertyDescription.isIndexed) {
          doc.add(new IntPoint(name, value));
        }
        if (propertyDescription.isStored) {
          doc.add(new StoredField(name, value));
        }
        if (propertyDescription.storeNumericDocValue) {
          doc.add(new NumericDocValuesField(name, value));
        }
      }
    },
    LONG("long") {
      @Override
      public void addField(
          Document doc, String name, Object v, PropertyDescription propertyDescription) {
        long value = (long) v;
        if (propertyDescription.isIndexed) {
          doc.add(new LongPoint(name, value));
        }
        if (propertyDescription.isStored) {
          doc.add(new StoredField(name, value));
        }
        if (propertyDescription.storeNumericDocValue) {
          doc.add(new NumericDocValuesField(name, value));
        }
      }
    },
    FLOAT("float") {
      @Override
      public void addField(
          Document doc, String name, Object v, PropertyDescription propertyDescription) {
        float value = (float) v;
        if (propertyDescription.isIndexed) {
          doc.add(new FloatPoint(name, value));
        }
        if (propertyDescription.isStored) {
          doc.add(new StoredField(name, value));
        }
        if (propertyDescription.storeNumericDocValue) {
          doc.add(new FloatDocValuesField(name, value));
        }
      }
    },
    DOUBLE("double") {
      @Override
      public void addField(
          Document doc, String name, Object v, PropertyDescription propertyDescription) {
        double value = (double) v;
        if (propertyDescription.isIndexed) {
          doc.add(new DoublePoint(name, value));
        }
        if (propertyDescription.isStored) {
          doc.add(new StoredField(name, value));
        }
        if (propertyDescription.storeNumericDocValue) {
          doc.add(new DoubleDocValuesField(name, value));
        }
      }
    },
    BOOLEAN("boolean") {
      @Override
      public void addField(
          Document doc, String name, Object value, PropertyDescription propertyDescription) {
        if ((boolean) value) {
          addStringProperty(doc, name, "true", propertyDescription);
        } else {
          addStringProperty(doc, name, "false", propertyDescription);
        }
      }
    };

    private final String name;

    PropertyType(String name) {
      this.name = name;
    }

    public abstract void addField(
        Document doc, String name, Object value, PropertyDescription propertyDescription);

    public String getName() {
      return name;
    }
  }

  /*
   * PropertyDescription describes various properties that can be set on a field.
   * TODO: Merge FieldDef and PropertyDescription?
   */
  static class PropertyDescription {
    public final PropertyType propertyType;
    public final boolean isStored;
    public final boolean isIndexed;
    public final boolean isAnalyzed;
    public final boolean storeNumericDocValue;

    PropertyDescription(
        PropertyType propertyType, boolean isStored, boolean isIndexed, boolean isAnalyzed) {
      this(propertyType, isStored, isIndexed, isAnalyzed, false);
    }

    PropertyDescription(
        PropertyType propertyType,
        boolean isStored,
        boolean isIndexed,
        boolean isAnalyzed,
        boolean storeNumericDocValue) {
      if (isAnalyzed && !isIndexed) {
        throw new InvalidPropertyDescriptionException(
            "Cannot set isAnalyzed without setting isIndexed");
      }

      if (isAnalyzed && !(propertyType.equals(PropertyType.TEXT))) {
        throw new InvalidPropertyDescriptionException(
            "Only text and any types can have isAnalyzed set");
      }

      this.propertyType = propertyType;
      this.isStored = isStored;
      this.isIndexed = isIndexed;
      this.isAnalyzed = isAnalyzed;
      this.storeNumericDocValue = storeNumericDocValue;
    }
  }

  // TODO: Map to FieldDef
  private static ImmutableMap<String, PropertyDescription> getDefaultPropertyDescriptions() {
    ImmutableMap.Builder<String, PropertyDescription> propertyDescriptionBuilder =
        ImmutableMap.builder();
    propertyDescriptionBuilder.put(
        LogMessage.SystemField.SOURCE.fieldName,
        new PropertyDescription(PropertyType.TEXT, true, false, false));
    propertyDescriptionBuilder.put(
        LogMessage.SystemField.ID.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));
    propertyDescriptionBuilder.put(
        LogMessage.SystemField.INDEX.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));
    propertyDescriptionBuilder.put(
        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        new PropertyDescription(PropertyType.LONG, false, true, false, true));
    propertyDescriptionBuilder.put(
        LogMessage.SystemField.TYPE.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));

    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.HOSTNAME.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.PACKAGE.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.MESSAGE.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.TAG.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.TIMESTAMP.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.USERNAME.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.PAYLOAD.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, false, false));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.NAME.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, false));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.SERVICE_NAME.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, false));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.DURATION_MS.fieldName,
        new PropertyDescription(PropertyType.LONG, false, true, false));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.TRACE_ID.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, false));
    propertyDescriptionBuilder.put(
        LogMessage.ReservedField.PARENT_ID.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, false));

    return propertyDescriptionBuilder.build();
  }

  // TODO: Should this be a per field policy? For now, may be keep it index level?
  public enum FieldConflictPolicy {
    // Throw an error on field conflict.
    RAISE_ERROR,
    // Drop the conflicting field.
    DROP_FIELD,
    // Convert the field value to the type of the conflicting field.
    CONVERT_FIELD_VALUE,
    // Covert the field value to the type of conflicting field and also create a new field of type.
    CONVERT_AND_DUPLICATE_FIELD
    // TODO: Consider another option where all conflicting fields are stored as strings. This
    //  option is simpler on the query side.
  }

  private static final Map<PropertyType, PropertyDescription> defaultPropDescriptionForType =
      ImmutableMap.of(
          PropertyType.LONG,
          new PropertyDescription(PropertyType.LONG, true, false, false, true),
          PropertyType.FLOAT,
          new PropertyDescription(PropertyType.FLOAT, true, false, false, true),
          PropertyType.INTEGER,
          new PropertyDescription(PropertyType.INTEGER, true, false, false, true),
          PropertyType.DOUBLE,
          new PropertyDescription(PropertyType.DOUBLE, true, false, false, true),
          PropertyType.TEXT,
          new PropertyDescription(PropertyType.TEXT, false, true, true));

  @VisibleForTesting
  public FieldConflictPolicy getIndexFieldConflictPolicy() {
    return indexFieldConflictPolicy;
  }

  public Map<String, PropertyDescription> getFieldDefMap() {
    return fieldDefMap;
  }

  private void addProperty(
      final Document doc, final String key, final Object value, final String keyPrefix) {
    // If value is a list, convert the value to a String and index the field.
    if (value instanceof List) {
      addProperty(doc, key, Strings.join((List) value, ','), keyPrefix);
      return;
    }

    String fieldName = keyPrefix.isBlank() || keyPrefix.isEmpty() ? key : keyPrefix + "." + key;
    // TODO: Add a depth limit for recursion.
    // Ingest nested map field recursively.
    if (value instanceof Map) {
      Map<Object, Object> mapValue = (Map<Object, Object>) value;
      for (Object k : mapValue.keySet()) {
        if (k instanceof String) {
          addProperty(doc, (String) k, mapValue.get(k), fieldName);
        } else {
          throw new PropertyTypeMismatchException(
              String.format(
                  "Property %s, %s has an non-string type which is unsupported", k, value));
        }
      }
      return;
    }

    PropertyType valueType = getJsonType(value);
    if (!fieldDefMap.containsKey(fieldName)) {
      indexNewField(doc, fieldName, value, valueType);
    } else {
      PropertyDescription registeredField = fieldDefMap.get(fieldName);
      if (registeredField.propertyType == valueType) {
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
                registeredField.propertyType);
            convertFieldValueCounter.increment();
            break;
          case CONVERT_AND_DUPLICATE_FIELD:
            convertValueAndIndexField(value, valueType, registeredField, doc, fieldName);
            LOG.debug(
                "Converting field {} value from type {} to {} due to type conflict",
                fieldName,
                valueType,
                registeredField.propertyType);
            // Add new field with new type
            String newFieldName = makeNewFieldOfType(fieldName, valueType);
            indexNewField(doc, newFieldName, value, valueType);
            LOG.debug(
                "Added new field {} of type {} due to type conflict", newFieldName, valueType);
            convertAndDuplicateFieldCounter.increment();
            break;
            // TODO: Another option is to duplicate field value only and not add?
          case RAISE_ERROR:
            throw new PropertyTypeMismatchException(
                String.format(
                    "Property type for field %s is %s but new value is of type  %s. ",
                    fieldName, registeredField.propertyType, valueType));
        }
      }
    }
  }

  private void indexNewField(Document doc, String key, Object value, PropertyType valueType) {
    // If we are seeing a field for the first time index it with default template for the
    // valueType and create a field def.
    if (!defaultPropDescriptionForType.containsKey(valueType)) {
      throw new RuntimeException("No default prop description");
    }

    PropertyDescription defaultPropDescription = defaultPropDescriptionForType.get(valueType);
    // add the document to this field.
    fieldDefMap.put(key, defaultPropDescription);
    indexTypedField(doc, key, value, defaultPropDescription);
  }

  static String makeNewFieldOfType(String key, PropertyType valueType) {
    return key + "_" + valueType.getName();
  }

  private static void convertValueAndIndexField(
      Object value,
      PropertyType valueType,
      PropertyDescription registeredField,
      Document doc,
      String key) {
    Object convertedValue = convertFieldValue(value, valueType, registeredField.propertyType);
    if (convertedValue == null) {
      throw new RuntimeException("No mapping found to convert value");
    }
    indexTypedField(doc, key, convertedValue, registeredField);
  }

  // TODO: Move to PropertyType?
  private static Object convertFieldValue(
      Object value, PropertyType fromType, PropertyType toType) {
    // String type
    if (fromType == PropertyType.TEXT) {
      if (toType == PropertyType.INTEGER) {
        return Integer.valueOf((String) value);
      }
      if (toType == PropertyType.LONG) {
        return Long.valueOf((String) value);
      }
      if (toType == PropertyType.FLOAT || toType == PropertyType.DOUBLE) {
        return Double.valueOf((String) value);
      }
    }

    // Int type
    if (fromType == PropertyType.INTEGER) {
      if (toType == PropertyType.TEXT) {
        return ((Integer) value).toString();
      }
      if (toType == PropertyType.LONG) {
        return ((Integer) value).longValue();
      }
      if (toType == PropertyType.FLOAT) {
        return ((Integer) value).floatValue();
      }
      if (toType == PropertyType.DOUBLE) {
        return ((Integer) value).doubleValue();
      }
    }
    return null;
  }

  private static void indexTypedField(
      Document doc, String key, Object value, PropertyDescription propertyDescription) {

    propertyDescription.propertyType.addField(doc, key, value, propertyDescription);

    // TODO: Add logic for map type.
    // TODO: Ignore exceptional fields when needed?
  }

  private static void addStringProperty(
      Document doc, String name, String value, PropertyDescription description) {

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

  private static PropertyType getJsonType(Object value) {
    if (value instanceof Long) {
      return PropertyType.LONG;
    }
    if (value instanceof Integer) {
      return PropertyType.INTEGER;
    }
    if (value instanceof String) {
      return PropertyType.TEXT;
    }
    if (value instanceof Float) {
      return PropertyType.FLOAT;
    }
    if (value instanceof Boolean) {
      return PropertyType.BOOLEAN;
    }
    if (value instanceof Double) {
      return PropertyType.DOUBLE;
    }

    // TODO: Handle other tyoes like map and list or nested objects?
    throw new RuntimeException("Unknown type");
  }

  public static SchemaAwareLogDocumentBuilderImpl build(
      FieldConflictPolicy fieldConflictPolicy, MeterRegistry meterRegistry) {
    // Add basic fields by default
    return new SchemaAwareLogDocumentBuilderImpl(
        fieldConflictPolicy, getDefaultPropertyDescriptions(), meterRegistry);
  }

  static final String DROP_FIELDS_COUNTER = "dropped_fields";
  static final String CONVERT_FIELD_VALUE_COUNTER = "convert_field_value";
  static final String CONVERT_AND_DUPLICATE_FIELD_COUNTER = "convert_and_duplicate_field";

  private final FieldConflictPolicy indexFieldConflictPolicy;
  private final Map<String, PropertyDescription> fieldDefMap = new ConcurrentHashMap<>();
  private final Counter droppedFieldsCounter;
  private final Counter convertFieldValueCounter;
  private final Counter convertAndDuplicateFieldCounter;

  SchemaAwareLogDocumentBuilderImpl(
      FieldConflictPolicy indexFieldConflictPolicy,
      final Map<String, PropertyDescription> initialFields,
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
    addProperty(doc, LogMessage.SystemField.INDEX.fieldName, message.getIndex(), "");
    addProperty(
        doc, LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, message.timeSinceEpochMilli, "");
    addProperty(doc, LogMessage.SystemField.TYPE.fieldName, message.getType(), "");
    addProperty(doc, LogMessage.SystemField.ID.fieldName, message.id, "");
    addProperty(
        doc,
        LogMessage.SystemField.SOURCE.fieldName,
        JsonUtil.writeAsString(message.toWireMessage()),
        "");
    for (String key : message.source.keySet()) {
      LOG.info("Adding key {}", key);
      addProperty(doc, key, message.source.get(key), "");
    }
    return doc;
  }
}
