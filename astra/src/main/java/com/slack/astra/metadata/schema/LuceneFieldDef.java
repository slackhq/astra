package com.slack.astra.metadata.schema;

import com.google.common.base.Objects;
import com.slack.astra.metadata.core.AstraMetadata;

/*
 * LuceneFieldDef describes the configs that can be set on a lucene field. This config defines how a field is indexed.
 */
public class LuceneFieldDef extends AstraMetadata {
  public final FieldType fieldType;
  public final boolean isStored;
  public final boolean isIndexed;
  public final boolean storeDocValue;

  public LuceneFieldDef(
      String name, String fieldType, boolean isStored, boolean isIndexed, boolean storeDocValue) {
    super(name);
    this.fieldType = FieldType.valueOf(fieldType.trim().toUpperCase());
    this.isStored = isStored;
    this.isIndexed = isIndexed;
    this.storeDocValue = storeDocValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LuceneFieldDef)) return false;
    if (!super.equals(o)) return false;
    LuceneFieldDef that = (LuceneFieldDef) o;
    return isStored == that.isStored
        && isIndexed == that.isIndexed
        && storeDocValue == that.storeDocValue
        && fieldType == that.fieldType;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), fieldType, isStored, isIndexed, storeDocValue);
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
        + ", storeDocValue="
        + storeDocValue
        + '}';
  }
}
