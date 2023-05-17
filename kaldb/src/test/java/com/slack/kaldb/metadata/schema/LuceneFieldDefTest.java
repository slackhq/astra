package com.slack.kaldb.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class LuceneFieldDefTest {
  @Test
  public void testLuceneFieldDef() {
    String intFieldName = "intField";
    String intType = "integer";

    LuceneFieldDef intField = new LuceneFieldDef(intFieldName, intType, true, false, false);
    assertThat(intField.name).isEqualTo(intFieldName);
    assertThat(intField.fieldType).isEqualTo(FieldType.INTEGER);
    assertThat(intField.isStored).isTrue();
    assertThat(intField.isIndexed).isFalse();
    assertThat(intField.storeDocValue).isFalse();

    String StrFieldName = "StrField";
    String strType = "text";
    LuceneFieldDef strField = new LuceneFieldDef(StrFieldName, strType, true, true, false);
    assertThat(strField.name).isEqualTo(StrFieldName);
    assertThat(strField.fieldType).isEqualTo(FieldType.TEXT);
    assertThat(strField.isStored).isTrue();
    assertThat(strField.isIndexed).isTrue();
    assertThat(strField.storeDocValue).isFalse();

    LuceneFieldDef boolField = new LuceneFieldDef("BooleanField", "boolean", true, true, false);
    assertThat(boolField.name).isEqualTo("BooleanField");
    assertThat(strField.fieldType).isEqualTo(FieldType.TEXT);
  }

  @Test
  public void testLuceneFieldDefEqualsHashcode() {
    String intFieldName = "intField";
    String intType = "integer";
    LuceneFieldDef intFieldA = new LuceneFieldDef(intFieldName, intType, true, false, false);
    LuceneFieldDef intFieldB = new LuceneFieldDef(intFieldName, intType, true, false, false);
    LuceneFieldDef intFieldC = new LuceneFieldDef("intFieldC", intType, true, false, false);
    LuceneFieldDef intFieldD = new LuceneFieldDef(intFieldName, intType, true, true, false);
    LuceneFieldDef intFieldE = new LuceneFieldDef(intFieldName, intType, true, true, true);

    assertThat(intFieldA).isEqualTo(intFieldB);
    assertThat(intFieldA).isNotEqualTo(intFieldC);
    assertThat(intFieldA).isNotEqualTo(intFieldD);
    assertThat(intFieldA).isNotEqualTo(intFieldE);

    assertThat(intFieldA.hashCode()).isEqualTo(intFieldB.hashCode());
    assertThat(intFieldA.hashCode()).isNotEqualTo(intFieldC.hashCode());
    assertThat(intFieldA.hashCode()).isNotEqualTo(intFieldD.hashCode());
    assertThat(intFieldA.hashCode()).isNotEqualTo(intFieldE.hashCode());
  }
}
