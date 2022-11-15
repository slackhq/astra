package com.slack.kaldb.metadata.schema;

import com.google.common.base.Objects;
import com.slack.kaldb.logstore.InvalidFieldDefException;
import com.slack.kaldb.metadata.core.KaldbMetadata;

/*
 * LuceneFieldDef describes the configs that can be set on a lucene field. This config defines how a field is indexed.
 */
public class LuceneFieldDef extends KaldbMetadata {
  public final FieldType fieldType;
  public final boolean isStored;
  public final boolean isIndexed;
  public final boolean isAnalyzed;
  public final boolean storeDocValue;

  public LuceneFieldDef(
      String name,
      String fieldType,
      boolean isStored,
      boolean isIndexed,
      boolean isAnalyzed,
      boolean storeDocValue) {
    super(name);
    this.fieldType = FieldType.valueOf(fieldType.trim().toUpperCase());
    if (isAnalyzed && !isIndexed) {
      throw new InvalidFieldDefException("Cannot set isAnalyzed without setting isIndexed");
    }

    if (isAnalyzed && !(this.fieldType.equals(FieldType.TEXT))) {
      throw new InvalidFieldDefException("Only text and any types can have isAnalyzed set");
    }

    //    if (storeDocValue
    //        && (this.fieldType.equals(FieldType.TEXT) ||
    // this.fieldType.equals(FieldType.BOOLEAN))) {
    //      throw new InvalidFieldDefException("Only numeric fields can have stored numeric doc
    // values");
    //    }

    this.isStored = isStored;
    this.isIndexed = isIndexed;
    this.isAnalyzed = isAnalyzed;
    this.storeDocValue = storeDocValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    LuceneFieldDef that = (LuceneFieldDef) o;
    return isStored == that.isStored
        && isIndexed == that.isIndexed
        && isAnalyzed == that.isAnalyzed
        && storeDocValue == that.storeDocValue
        && fieldType == that.fieldType;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(), fieldType, isStored, isIndexed, isAnalyzed, storeDocValue);
  }

  @Override
  public String toString() {
    return "LuceneFieldDef{"
        + "name='"
        + name
        + '\''
        + ", fieldType="
        + fieldType
        + ", isStored="
        + isStored
        + ", isIndexed="
        + isIndexed
        + ", isAnalyzed="
        + isAnalyzed
        + ", storeDocValue="
        + storeDocValue
        + '}';
  }
}
