package com.slack.kaldb.metadata.schema;

import com.google.common.base.Objects;
import com.slack.kaldb.logstore.InvalidFieldDefException;
import com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.kaldb.metadata.core.KaldbMetadata;

/*
 * FieldDef describes the configs that can be set on a field.
 */
public class LuceneFieldDefMetadata extends KaldbMetadata {
  public final SchemaAwareLogDocumentBuilderImpl.FieldType fieldType;
  public final boolean isStored;
  public final boolean isIndexed;
  public final boolean isAnalyzed;
  public final boolean storeNumericDocValue;

  public LuceneFieldDefMetadata(
      String name,
      String fieldType,
      boolean isStored,
      boolean isIndexed,
      boolean isAnalyzed,
      boolean storeNumericDocValue) {
    super(name);
    this.fieldType =
        SchemaAwareLogDocumentBuilderImpl.FieldType.valueOf(fieldType.trim().toUpperCase());
    if (isAnalyzed && !isIndexed) {
      throw new InvalidFieldDefException("Cannot set isAnalyzed without setting isIndexed");
    }

    if (isAnalyzed && !(this.fieldType.equals(SchemaAwareLogDocumentBuilderImpl.FieldType.TEXT))) {
      throw new InvalidFieldDefException("Only text and any types can have isAnalyzed set");
    }

    if (storeNumericDocValue
        && (this.fieldType.equals(SchemaAwareLogDocumentBuilderImpl.FieldType.TEXT)
            || this.fieldType.equals(SchemaAwareLogDocumentBuilderImpl.FieldType.BOOLEAN))) {
      throw new InvalidFieldDefException("Only numeric fields can have stored numeric doc values");
    }

    this.isStored = isStored;
    this.isIndexed = isIndexed;
    this.isAnalyzed = isAnalyzed;
    this.storeNumericDocValue = storeNumericDocValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    LuceneFieldDefMetadata that = (LuceneFieldDefMetadata) o;
    return isStored == that.isStored
        && isIndexed == that.isIndexed
        && isAnalyzed == that.isAnalyzed
        && storeNumericDocValue == that.storeNumericDocValue
        && fieldType == that.fieldType;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(), fieldType, isStored, isIndexed, isAnalyzed, storeNumericDocValue);
  }

  @Override
  public String toString() {
    return "LuceneFieldDefMetadata{"
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
        + ", storeNumericDocValue="
        + storeNumericDocValue
        + '}';
  }
}
