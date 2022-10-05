package com.slack.kaldb.logstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.slack.kaldb.util.JsonUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 */
public class SchemaAwareLogDocumentBuilderImpl implements DocumentBuilder<LogMessage> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SchemaAwareLogDocumentBuilderImpl.class);

  // TODO: Add abstract methods to enum to structure fields better.
  // TODO: Add a string field name which is a string.
  public enum PropertyType {
    TEXT("text") {
      @Override
      public Object toType(PropertyType toType) {
        return null;
      }

      @Override
      public void addField(
          Document doc, String name, Object value, PropertyDescription propertyDescription) {
        addStringProperty(doc, name, (String) value, propertyDescription);
      }
    },

    INTEGER("integer") {
      @Override
      public Object toType(PropertyType toType) {
        return null;
      }

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
      public Object toType(PropertyType toType) {
        return null;
      }

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
      public Object toType(PropertyType toType) {
        return null;
      }

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
      public Object toType(PropertyType toType) {
        return null;
      }

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
      public Object toType(PropertyType toType) {
        return null;
      }

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

    public abstract Object toType(PropertyType toType);

    public abstract void addField(
        Document doc, String name, Object value, PropertyDescription propertyDescription);
  }

  /*
   * PropertyDescription describes various properties that can be set on a field.
   */
  private static class PropertyDescription {
    final PropertyType propertyType;
    final boolean isStored;
    final boolean isIndexed;
    final boolean isAnalyzed;
    final boolean storeNumericDocValue;

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

  static class FieldDef {
    public final PropertyType type;
    public final PropertyDescription propertyDescription;
    // No per field, field conflict policy for now. But placeholder for future.
    // public final FieldConflictPolicy fieldConflictPolicy = null;

    FieldDef(PropertyType type, PropertyDescription propertyDescription) {
      this.type = type;
      this.propertyDescription = propertyDescription;
    }
  }

  // TODO: Should this be a per field policy? For now, may be keep it index level?
  public enum FieldConflictPolicy {
    DROP_FIELD,
    CONVERT_FIELD_VALUE,
    CONVERT_AND_DUPLICATE_FIELD
  }

  // TODO: Add other types. Make immutable.
  private static final Map<PropertyType, PropertyDescription> defaultPropDescriptionForType =
      Map.of(
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

  public FieldConflictPolicy getIndexFieldConflictPolicy() {
    return indexFieldConflictPolicy;
  }

  public Map<String, FieldDef> getFieldDefMap() {
    return fieldDefMap;
  }

  private void addProperty(Document doc, String key, Object value) {
    PropertyType valueType = getJsonType(value);
    if (!fieldDefMap.containsKey(key)) {
      indexNewField(doc, key, value, valueType);
    } else {
      FieldDef registeredField = fieldDefMap.get(key);
      if (registeredField.type == valueType) {
        // No field conflicts index it using previous description.
        indexTypedField(doc, key, value, registeredField.propertyDescription);
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
          indexNewField(doc, newFieldName, value, valueType);
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
    fieldDefMap.put(key, new FieldDef(valueType, defaultPropDescription));
    indexTypedField(doc, key, value, defaultPropDescription);
  }

  static String makeNewFieldOfType(String key, PropertyType valueType) {
    return key + "_" + valueType.name();
  }

  private static void convertValueAndIndexField(
      Object value, PropertyType valueType, FieldDef registeredField, Document doc, String key) {
    Object convertedValue = convertFieldValue(value, valueType, registeredField.type);
    if (convertedValue == null) {
      throw new RuntimeException("No mapping found to convert value");
    }
    indexTypedField(doc, key, convertedValue, registeredField.propertyDescription);
  }

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

  private final FieldConflictPolicy indexFieldConflictPolicy;
  private final Map<String, FieldDef> fieldDefMap = new ConcurrentHashMap<>();

  SchemaAwareLogDocumentBuilderImpl(
      FieldConflictPolicy indexFieldConflictPolicy, final Map<String, FieldDef> initialFields) {
    this.indexFieldConflictPolicy = indexFieldConflictPolicy;
    fieldDefMap.putAll(initialFields);
  }

  public static SchemaAwareLogDocumentBuilderImpl build(FieldConflictPolicy fieldConflictPolicy) {
    Map<String, FieldDef> initialFields = new HashMap<>();
    // Add default fields and their property descriptions
    getDefaultPropertyDescriptions()
        .forEach((k, v) -> initialFields.put(k, new FieldDef(v.propertyType, v)));
    return new SchemaAwareLogDocumentBuilderImpl(fieldConflictPolicy, initialFields);
  }

  @Override
  public Document fromMessage(LogMessage message) throws JsonProcessingException {
    Document doc = new Document();
    addProperty(doc, LogMessage.SystemField.INDEX.fieldName, message.getIndex());
    addProperty(
        doc, LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, message.timeSinceEpochMilli);
    addProperty(doc, LogMessage.SystemField.TYPE.fieldName, message.getType());
    addProperty(doc, LogMessage.SystemField.ID.fieldName, message.id);
    addProperty(
        doc,
        LogMessage.SystemField.SOURCE.fieldName,
        JsonUtil.writeAsString(message.toWireMessage()));
    for (String key : message.source.keySet()) {
      LOG.info("Adding key {}", key);
      addProperty(doc, key, message.source.get(key));
    }
    return doc;
  }
}
