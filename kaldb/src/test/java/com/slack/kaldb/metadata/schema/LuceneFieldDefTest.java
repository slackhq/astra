package com.slack.kaldb.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.slack.kaldb.logstore.InvalidFieldDefException;
import org.junit.Test;

public class LuceneFieldDefTest {
  @Test
  public void testLuceneFieldDef() {
    String intFieldName = "intField";
    String intType = "integer";

    LuceneFieldDef intField = new LuceneFieldDef(intFieldName, intType, true, false, false, false);
    assertThat(intField.name).isEqualTo(intFieldName);
    assertThat(intField.fieldType).isEqualTo(FieldType.INTEGER);
    assertThat(intField.isStored).isTrue();
    assertThat(intField.isIndexed).isFalse();
    assertThat(intField.isAnalyzed).isFalse();
    assertThat(intField.storeDocValue).isFalse();

    String StrFieldName = "StrField";
    String strType = "text";
    LuceneFieldDef strField = new LuceneFieldDef(StrFieldName, strType, true, true, true, false);

    assertThat(strField.name).isEqualTo(StrFieldName);
    assertThat(strField.fieldType).isEqualTo(FieldType.TEXT);
    assertThat(strField.isStored).isTrue();
    assertThat(strField.isIndexed).isTrue();
    assertThat(strField.isAnalyzed).isTrue();
    assertThat(strField.storeDocValue).isFalse();

    LuceneFieldDef boolField =
        new LuceneFieldDef("BooleanField", "boolean", true, true, false, false);
    assertThat(boolField.name).isEqualTo("BooleanField");
    assertThat(strField.fieldType).isEqualTo(FieldType.TEXT);
  }

  @Test
  public void testLuceneFieldDefEqualsHashcode() {
    String intFieldName = "intField";
    String intType = "integer";
    LuceneFieldDef intFieldA = new LuceneFieldDef(intFieldName, intType, true, false, false, false);
    LuceneFieldDef intFieldB = new LuceneFieldDef(intFieldName, intType, true, false, false, false);
    LuceneFieldDef intFieldC = new LuceneFieldDef("intFieldC", intType, true, false, false, false);
    LuceneFieldDef intFieldD = new LuceneFieldDef(intFieldName, intType, true, true, false, false);
    LuceneFieldDef intFieldE = new LuceneFieldDef(intFieldName, intType, true, true, false, true);

    assertThat(intFieldA).isEqualTo(intFieldB);
    assertThat(intFieldA).isNotEqualTo(intFieldC);
    assertThat(intFieldA).isNotEqualTo(intFieldD);
    assertThat(intFieldA).isNotEqualTo(intFieldE);

    assertThat(intFieldA.hashCode()).isEqualTo(intFieldB.hashCode());
    assertThat(intFieldA.hashCode()).isNotEqualTo(intFieldC.hashCode());
    assertThat(intFieldA.hashCode()).isNotEqualTo(intFieldD.hashCode());
    assertThat(intFieldA.hashCode()).isNotEqualTo(intFieldE.hashCode());
  }

  @Test
  public void invalidArgumentsShouldThrow() {
    assertThatThrownBy(() -> new LuceneFieldDef("intField", "integer", true, false, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(() -> new LuceneFieldDef("longField", "long", true, false, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(() -> new LuceneFieldDef("longField", "long", true, true, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(() -> new LuceneFieldDef("floatField", "float", true, true, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(() -> new LuceneFieldDef("DoubleField", "double", true, true, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(() -> new LuceneFieldDef("BooleanField", "boolean", true, true, true, true))
        .isInstanceOf(InvalidFieldDefException.class);
  }
}
