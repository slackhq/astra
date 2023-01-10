package com.slack.kaldb.metadata.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.BytesRef;

/** The FieldType enum describes the types of fields in a chunk. */
public enum FieldType {
  TEXT("text") {
    @Override
    public void addField(Document doc, String name, Object value, LuceneFieldDef fieldDef) {
      if (fieldDef.isIndexed) {
        doc.add(new TextField(name, (String) value, getStoreEnum(fieldDef.isStored)));
      }
      if (fieldDef.isStored) {
        doc.add(new StoredField(name, (String) value));
      }
      if (fieldDef.storeDocValue) {
        // Since a text field is tokenized, we don't need to add doc values to it.
      }
    }
  },
  STRING("string") {
    @Override
    public void addField(Document doc, String name, Object value, LuceneFieldDef fieldDef) {
      if (fieldDef.isIndexed) {
        doc.add(new StringField(name, (String) value, getStoreEnum(fieldDef.isStored)));
      }
      if (fieldDef.isStored) {
        doc.add(new StoredField(name, (String) value));
      }
      if (fieldDef.storeDocValue) {
        doc.add(new SortedDocValuesField(name, new BytesRef((String) value)));
      }
    }
  },
  INTEGER("integer") {
    @Override
    public void addField(Document doc, String name, Object v, LuceneFieldDef fieldDef) {
      int value = (int) v;
      if (fieldDef.isIndexed) {
        doc.add(new IntPoint(name, value));
      }
      if (fieldDef.isStored) {
        doc.add(new StoredField(name, value));
      }
      if (fieldDef.storeDocValue) {
        doc.add(new NumericDocValuesField(name, value));
      }
    }
  },
  LONG("long") {
    @Override
    public void addField(Document doc, String name, Object v, LuceneFieldDef fieldDef) {
      long value = (long) v;
      if (fieldDef.isIndexed) {
        doc.add(new LongPoint(name, value));
      }
      if (fieldDef.isStored) {
        doc.add(new StoredField(name, value));
      }
      if (fieldDef.storeDocValue) {
        doc.add(new NumericDocValuesField(name, value));
      }
    }
  },
  FLOAT("float") {
    @Override
    public void addField(Document doc, String name, Object v, LuceneFieldDef fieldDef) {
      float value = (float) v;
      if (fieldDef.isIndexed) {
        doc.add(new FloatPoint(name, value));
      }
      if (fieldDef.isStored) {
        doc.add(new StoredField(name, value));
      }
      if (fieldDef.storeDocValue) {
        doc.add(new FloatDocValuesField(name, value));
      }
    }
  },
  DOUBLE("double") {
    @Override
    public void addField(Document doc, String name, Object v, LuceneFieldDef fieldDef) {
      double value = (double) v;
      if (fieldDef.isIndexed) {
        doc.add(new DoublePoint(name, value));
      }
      if (fieldDef.isStored) {
        doc.add(new StoredField(name, value));
      }
      if (fieldDef.storeDocValue) {
        doc.add(new DoubleDocValuesField(name, value));
      }
    }
  },
  BOOLEAN("boolean") {
    @Override
    public void addField(Document doc, String name, Object value, LuceneFieldDef fieldDef) {
      // Lucene has no native support for Booleans so store that field as text.
      String valueStr = String.valueOf((boolean) value);
      if (fieldDef.isIndexed) {
        doc.add(new StringField(name, valueStr, getStoreEnum(fieldDef.isStored)));
      }
      if (fieldDef.isStored) {
        doc.add(new StoredField(name, valueStr));
      }
      if (fieldDef.storeDocValue) {
        doc.add(new SortedDocValuesField(name, new BytesRef(valueStr)));
      }
    }
  };

  public final String name;

  FieldType(String name) {
    this.name = name;
  }

  public abstract void addField(Document doc, String name, Object value, LuceneFieldDef fieldDef);

  public String getName() {
    return name;
  }

  @VisibleForTesting
  public static Object convertFieldValue(Object value, FieldType fromType, FieldType toType) {
    if ((fromType == toType)
        || (fromType == FieldType.TEXT && toType == FieldType.STRING)
        || (fromType == FieldType.STRING && toType == FieldType.TEXT)) {
      return value;
    }

    if (fromType == FieldType.TEXT || fromType == FieldType.STRING) {
      if (toType == FieldType.INTEGER) {
        try {
          return Integer.valueOf((String) value);
        } catch (NumberFormatException e) {
          return 0;
        }
      }
      if (toType == FieldType.LONG) {
        try {
          return Long.valueOf((String) value);
        } catch (NumberFormatException e) {
          return (long) 0;
        }
      }
      if (toType == FieldType.DOUBLE) {
        try {
          return Double.valueOf((String) value);
        } catch (NumberFormatException e) {
          return (double) 0;
        }
      }
      if (toType == FieldType.FLOAT) {
        try {
          return Float.valueOf((String) value);
        } catch (NumberFormatException e) {
          return (float) 0;
        }
      }
      if (toType == FieldType.BOOLEAN) {
        return ((String) value).equals("1") || ((String) value).equalsIgnoreCase("true");
      }
    }

    // Int type
    if (fromType == FieldType.INTEGER) {
      if (toType == FieldType.TEXT || toType == FieldType.STRING) {
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
      if (toType == FieldType.BOOLEAN) {
        return ((Integer) value) != 0;
      }
    }

    // Long type
    if (fromType == FieldType.LONG) {
      if (toType == FieldType.TEXT || toType == FieldType.STRING) {
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
      if (toType == FieldType.BOOLEAN) {
        return ((Long) value) != 0;
      }
    }

    // Float type
    if (fromType == FieldType.FLOAT) {
      if (toType == FieldType.TEXT || toType == FieldType.STRING) {
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
      if (toType == FieldType.BOOLEAN) {
        return ((Float) value) != 0;
      }
    }

    // Double type
    if (fromType == FieldType.DOUBLE) {
      if (toType == FieldType.TEXT || toType == FieldType.STRING) {
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
      if (toType == FieldType.BOOLEAN) {
        return ((Double) value) != 0;
      }
    }

    if (fromType == FieldType.BOOLEAN) {
      if (toType == FieldType.TEXT || toType == FieldType.STRING) {
        return value.toString();
      }
      if (toType == FieldType.INTEGER) {
        return (Boolean) value ? 1 : 0;
      }
      if (toType == FieldType.LONG) {
        return (Boolean) value ? 1L : 0L;
      }
      if (toType == FieldType.FLOAT) {
        return (Boolean) value ? 1f : 0f;
      }
      if (toType == FieldType.DOUBLE) {
        return (Boolean) value ? 1d : 0d;
      }
    }
    return null;
  }

  private static Field.Store getStoreEnum(boolean isStored) {
    return isStored ? Field.Store.YES : Field.Store.NO;
  }

  // Aliased Field Types are FieldTypes that can be considered as same type from a field conflict
  // detection perspective
  public static final List<Set<FieldType>> ALIASED_FIELD_TYPES =
      ImmutableList.of(ImmutableSet.of(FieldType.STRING, FieldType.TEXT));

  public static boolean areTypeAliasedFieldTypes(FieldType type1, FieldType type2) {
    for (Set<FieldType> s : ALIASED_FIELD_TYPES) {
      if (s.contains(type1) && s.contains(type2)) return true;
    }
    return false;
  }
}
