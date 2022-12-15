package com.slack.kaldb.logstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import com.slack.kaldb.util.JsonUtil;
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
 * LogDocumentBuilder translates a LogMessage into a lucene Document - a bag of fields, that is
 * indexed and stored by lucene. DocumentBuilder takes in a default treatment for a property in our
 * JSON and a field specific override. In addition, we always add the "all" system field with the
 * entire json field "unindexed".
 *
 * <p>TODO: Add a benchmark for the _all field to understand the cpu and storage overhead better.
 */
public class LogDocumentBuilderImpl implements DocumentBuilder<LogMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(LogDocumentBuilderImpl.class);

  enum PropertyType {
    TEXT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    ANY
  }

  /*
   * PropertyDescription describes various properties that can be set on a field.
   */
  static class PropertyDescription {
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

  public static DocumentBuilder<LogMessage> build(
      boolean ignoreExceptions, boolean enableFullTextSearch) {
    ImmutableMap.Builder<String, PropertyDescription> propertyDescriptionBuilder =
        ImmutableMap.builder();
    propertyDescriptionBuilder.put(
        LogMessage.SystemField.SOURCE.fieldName,
        new PropertyDescription(PropertyType.TEXT, true, false, false));
    propertyDescriptionBuilder.put(
        LogMessage.SystemField.ALL.fieldName,
        new PropertyDescription(PropertyType.TEXT, false, true, true));
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

    PropertyDescription defaultDescription =
        new PropertyDescription(PropertyType.ANY, false, true, true);
    return new LogDocumentBuilderImpl(
        ignoreExceptions,
        propertyDescriptionBuilder.build(),
        defaultDescription,
        enableFullTextSearch);
  }

  private final boolean ignorePropertyTypeExceptions;
  private final boolean enableFullTextSearch;
  private final PropertyDescription defaultDescription;
  private final Map<String, PropertyDescription> propertyDescriptions;

  public LogDocumentBuilderImpl(
      boolean ignorePropertyTypeExceptions,
      Map<String, PropertyDescription> propertyDescriptions,
      PropertyDescription defaultDescription,
      boolean enableFullTextSearch) {
    this.ignorePropertyTypeExceptions = ignorePropertyTypeExceptions;
    this.propertyDescriptions = propertyDescriptions;
    this.defaultDescription = defaultDescription;
    this.enableFullTextSearch = enableFullTextSearch;
    LOG.info(
        "Started document builder with config ignoreExceptions: {} and enableFullTextSearch: {}",
        ignorePropertyTypeExceptions,
        enableFullTextSearch);
  }

  private PropertyDescription getDescription(String propertyName) {
    return propertyDescriptions.getOrDefault(propertyName, defaultDescription);
  }

  private Field.Store getStoreEnum(boolean isStored) {
    return isStored ? Field.Store.YES : Field.Store.NO;
  }

  private void addStringProperty(
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

  public void addProperty(Document doc, String name, Object value) {
    PropertyDescription desc = getDescription(name);

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
      addProperty(doc, name, String.valueOf((boolean) value));
      return;
    }

    // Add a map of properties at once.
    if (value instanceof Map) {
      Map<Object, Object> mapValue = (Map<Object, Object>) value;
      for (Object k : mapValue.keySet()) {
        if (k instanceof String) {
          addPropertyHandleExceptions(doc, (String) k, mapValue.get(k));
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

  private void addPropertyHandleExceptions(Document doc, String name, Object value) {
    try {
      addProperty(doc, name, value);
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

  @Override
  public String toString() {
    return "LogDocumentBuilderImpl{"
        + "ignorePropertyTypeExceptions="
        + ignorePropertyTypeExceptions
        + ", defaultDescription="
        + defaultDescription
        + ", propertyDescriptions="
        + propertyDescriptions
        + '}';
  }

  @Override
  public Document fromMessage(LogMessage message) throws JsonProcessingException {
    Document doc = new Document();
    addProperty(doc, LogMessage.SystemField.INDEX.fieldName, message.getIndex());
    addProperty(
        doc, LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, message.timeSinceEpochMilli);
    addProperty(doc, LogMessage.SystemField.TYPE.fieldName, message.getType());
    addProperty(doc, LogMessage.SystemField.ID.fieldName, message.id);
    final String msgString = JsonUtil.writeAsString(message.toWireMessage());
    addProperty(doc, LogMessage.SystemField.SOURCE.fieldName, msgString);
    if (enableFullTextSearch) {
      addProperty(doc, LogMessage.SystemField.ALL.fieldName, msgString);
    }
    for (String key : message.source.keySet()) {
      addPropertyHandleExceptions(doc, key, message.source.get(key));
    }
    return doc;
  }

  @Override
  public ConcurrentHashMap<String, LuceneFieldDef> getSchema() {
    // Report empty map since this class doesn't support schemas.
    return new ConcurrentHashMap<>();
  }
}
