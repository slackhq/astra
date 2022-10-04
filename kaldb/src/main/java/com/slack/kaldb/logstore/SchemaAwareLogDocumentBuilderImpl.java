package com.slack.kaldb.logstore;

import com.google.common.collect.ImmutableMap;
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
class SchemaAwareLogDocumentBuilderImpl implements DocumentBuilder<LogMessage> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SchemaAwareLogDocumentBuilderImpl.class);

  private static final PropertyDescription DEFAULT_PROPERTY_DESCRIPTION =
      new PropertyDescription(PropertyType.ANY, false, true, true);

  private enum PropertyType {
    TEXT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    ANY
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

      if (isAnalyzed
          && !(propertyType.equals(PropertyType.TEXT) || propertyType.equals(PropertyType.ANY))) {
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
  enum FieldConflictPolicy {
    DROP_FIELD,
    CONVERT_FIELD_VALUE,
    CONVERT_AND_DUPLICATE_FIELD
  }

  // TODO: Add other types. Make immutable.
  private static final Map<String, PropertyDescription> defaultPropDescriptionForType =
      Map.of(
          "Long",
          new PropertyDescription(PropertyType.LONG, false, true, false, true),
          "Float",
          new PropertyDescription(PropertyType.FLOAT, false, true, false, true),
          "Int",
          new PropertyDescription(PropertyType.INTEGER, false, true, false, true),
          "Double",
          new PropertyDescription(PropertyType.DOUBLE, false, true, false, true),
          "String",
          new PropertyDescription(PropertyType.TEXT, false, true, true));

  private final FieldConflictPolicy indexFieldConflictPolicy;
  private final Map<String, FieldDef> fieldDefMap = new ConcurrentHashMap<>();

  SchemaAwareLogDocumentBuilderImpl(FieldConflictPolicy indexFieldConflictPolicy) {
    this.indexFieldConflictPolicy = indexFieldConflictPolicy;
    // Add default fields and their property descriptions
    getDefaultPropertyDescriptions()
        .forEach((k, v) -> fieldDefMap.put(k, new FieldDef(v.propertyType, v)));
  }

  @Override
  public Document fromMessage(LogMessage message) {
    Document doc = new Document();
    for (String key : message.source.keySet()) {
      addProperty(doc, key, message.source.get(key));
    }
    return doc;
  }

  private void addProperty(Document doc, String key, Object value) {
    PropertyType valueType = getJsonType(value);
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

  private void addFieldAndIndexField(
      Document doc, String key, Object value, PropertyType valueType) {
    // If we are seeing a field for the first time index it with default template for the
    // valueType and create a field def.
    if (!defaultPropDescriptionForType.containsKey(valueType)) {
      throw new RuntimeException("No default prop description");
    }

    PropertyDescription defaultPropDescription = defaultPropDescriptionForType.get(valueType);
    // add the document to this field.
    fieldDefMap.put(key, new FieldDef(valueType, defaultPropDescription));
    indexFieldWithType(doc, key, value, defaultPropDescription);
  }

  private String makeNewFieldOfType(String key, PropertyType valueType) {
    return key + "_" + valueType.name();
  }

  private void convertValueAndIndexField(
      Object value, PropertyType valueType, FieldDef registeredField, Document doc, String key) {
    Object convertedValue = convertFieldValue(value, valueType, registeredField.type);
    if (convertedValue == null) {
      throw new RuntimeException("No mapping found to convert value");
    }
    indexFieldWithType(doc, key, convertedValue, registeredField.propertyDescription);
  }

  private Object convertFieldValue(Object value, PropertyType fromType, PropertyType toType) {
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

  private void indexFieldWithType(
      Document doc, String key, Object value, PropertyDescription propertyDescription) {
    if (value instanceof String) {
      addStringProperty(doc, key, (String) value, propertyDescription);
    }
  }

  // TODO: Return enum or java class.
  private PropertyType getJsonType(Object value) {
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
    // Fix this.
    throw new RuntimeException("Unknown type");
  }

  @SuppressWarnings("unchecked")
  private static void addProperty(
      Document doc,
      String name,
      Object value,
      Map<String, PropertyDescription> propertyDescriptions,
      boolean ignorePropertyTypeExceptions) {
    PropertyDescription desc =
        propertyDescriptions.getOrDefault(name, DEFAULT_PROPERTY_DESCRIPTION);

    // Match string
    if (value instanceof String) {
      if (!(desc.propertyType.equals(PropertyType.ANY)
          || desc.propertyType.equals(PropertyType.TEXT))) {
        throw new PropertyTypeMismatchException(
            String.format("Found string but property %s was not configured as TextProperty", name));
      }
      if (desc.storeNumericDocValue) {
        throw new PropertyTypeMismatchException(
            String.format(
                "Found string but property %s was configured with storeNumericDocValue=true.",
                name));
      }
      addStringProperty(doc, name, (String) value, desc);
      return;
    }

    // Match int
    if (value instanceof Integer) {
      int intValue = (Integer) value;
      if (desc.propertyType.equals(PropertyType.INTEGER)) {
        // TODO: Add a test to ensure IntPoint works as well as IntField.
        // TODO: GetStoreEnum field is missing as a param.
        if (desc.isIndexed) {
          doc.add(new IntPoint(name, intValue));
        }
        if (desc.isStored) {
          doc.add(new StoredField(name, intValue));
        }
        if (desc.storeNumericDocValue) {
          doc.add(new NumericDocValuesField(name, intValue));
        }
      } else if (desc.propertyType.equals(PropertyType.ANY)) {
        // Treat integers as a string in this case because Lucene QueryParser doesn't numeric types
        addStringProperty(doc, name, String.valueOf(intValue), desc);
      } else {
        throw new PropertyTypeMismatchException(
            String.format(
                "Found int but property %s was not configured to be an int property.", name));
      }
      return;
    }

    // Match long
    if (value instanceof Long) {
      long longValue = (Long) value;
      if (desc.propertyType.equals(PropertyType.LONG)) {
        if (desc.isIndexed) {
          doc.add(new LongPoint(name, longValue));
        }
        if (desc.isStored) {
          doc.add(new StoredField(name, longValue));
        }
        if (desc.storeNumericDocValue) {
          doc.add(new NumericDocValuesField(name, longValue));
        }
      } else if (desc.propertyType.equals(PropertyType.ANY)) {
        // Treat longs as strings in this case since LuceneQueryParser doesn't understand numeric
        // types.
        addStringProperty(doc, name, String.valueOf(longValue), desc);
      } else {
        throw new PropertyTypeMismatchException(
            String.format("Found long but property %s was not configured to be a Long.", name));
      }
      return;
    }

    // Match float
    if (value instanceof Float) {
      float floatValue = (Float) value;
      if (desc.propertyType.equals(PropertyType.FLOAT)) {
        if (desc.isIndexed) {
          doc.add(new FloatPoint(name, floatValue));
        }
        if (desc.isStored) {
          doc.add(new StoredField(name, floatValue));
        }
        if (desc.storeNumericDocValue) {
          doc.add(new FloatDocValuesField(name, floatValue));
        }
      } else if (desc.propertyType.equals(PropertyType.ANY)) {
        // Treat floats as strings in this case since LuceneQueryParser doesn't understand numeric
        // types.
        addStringProperty(doc, name, String.valueOf(floatValue), desc);
      } else {
        throw new PropertyTypeMismatchException(
            String.format("Found float but property %s was not configured to be a Float.", name));
      }
      return;
    }

    // Match double
    if (value instanceof Double) {
      double doubleValue = (Double) value;
      if (desc.propertyType.equals(PropertyType.DOUBLE)) {
        if (desc.isIndexed) {
          doc.add(new DoublePoint(name, doubleValue));
        }
        if (desc.isStored) {
          doc.add(new StoredField(name, doubleValue));
        }
        if (desc.storeNumericDocValue) {
          doc.add(new DoubleDocValuesField(name, doubleValue));
        }
      } else if (desc.propertyType.equals(PropertyType.ANY)) {
        // Treat doubles  as strings in this case since LuceneQueryParser doesn't understand numeric
        // types.
        addStringProperty(doc, name, String.valueOf(doubleValue), desc);
      } else {
        throw new PropertyTypeMismatchException(
            String.format("Found double but property %s was not configured to be a Double.", name));
      }
      return;
    }

    // Match boolean
    if (value instanceof Boolean) {
      addProperty(
          doc,
          name,
          String.valueOf((boolean) value),
          propertyDescriptions,
          ignorePropertyTypeExceptions);
      return;
    }

    // Add a map of properties at once.
    if (value instanceof Map) {
      Map<Object, Object> mapValue = (Map<Object, Object>) value;
      for (Object k : mapValue.keySet()) {
        if (k instanceof String) {
          addPropertyHandleExceptions(
              doc, (String) k, mapValue.get(k), propertyDescriptions, ignorePropertyTypeExceptions);
        } else {
          throw new PropertyTypeMismatchException(
              String.format(
                  "Property %s,%s has an non-string type which is unsupported", name, value));
        }
      }
      return;
    }

    // Unknown type.
    throw new UnSupportedPropertyTypeException(
        String.format("Property %s, %s has unsupported type.", name, value));
  }

  private static Field.Store getStoreEnum(boolean isStored) {
    return isStored ? Field.Store.YES : Field.Store.NO;
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

  private static void addPropertyHandleExceptions(
      Document doc,
      String name,
      Object value,
      Map<String, PropertyDescription> propertyDescriptions,
      boolean ignorePropertyTypeExceptions) {
    try {
      addProperty(doc, name, value, propertyDescriptions, ignorePropertyTypeExceptions);
    } catch (UnSupportedPropertyTypeException u) {
      if (ignorePropertyTypeExceptions) {
        LOG.debug(u.toString());
      } else {
        throw u;
      }
    } catch (PropertyTypeMismatchException p) {
      if (ignorePropertyTypeExceptions) {
        LOG.error("Property type mismatch", p);
      } else {
        throw p;
      }
    }
  }
}
