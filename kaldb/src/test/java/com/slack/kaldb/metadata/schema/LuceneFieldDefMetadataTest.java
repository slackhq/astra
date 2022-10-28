package com.slack.kaldb.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.slack.kaldb.logstore.InvalidFieldDefException;
import com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldType;
import org.junit.Test;

public class LuceneFieldDefMetadataTest {
  @Test
  public void testLuceneFieldDefMetadata() {
    String intFieldName = "intField";
    String intType = "integer";

    LuceneFieldDefMetadata intFieldMetadata =
        new LuceneFieldDefMetadata(intFieldName, intType, true, false, false, false);
    assertThat(intFieldMetadata.name).isEqualTo(intFieldName);
    assertThat(intFieldMetadata.fieldType)
        .isEqualTo(FieldType.INTEGER);
    assertThat(intFieldMetadata.isStored).isTrue();
    assertThat(intFieldMetadata.isIndexed).isFalse();
    assertThat(intFieldMetadata.isAnalyzed).isFalse();
    assertThat(intFieldMetadata.storeNumericDocValue).isFalse();

    String StrFieldName = "StrField";
    String strType = "text";
    LuceneFieldDefMetadata strFieldMetadata =
        new LuceneFieldDefMetadata(StrFieldName, strType, true, true, true, false);

    assertThat(strFieldMetadata.name).isEqualTo(StrFieldName);
    assertThat(strFieldMetadata.fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(strFieldMetadata.isStored).isTrue();
    assertThat(strFieldMetadata.isIndexed).isTrue();
    assertThat(strFieldMetadata.isAnalyzed).isTrue();
    assertThat(strFieldMetadata.storeNumericDocValue).isFalse();


    LuceneFieldDefMetadata boolField = new LuceneFieldDefMetadata("BooleanField", "boolean", true, true, false, false);
    assertThat(boolField.name).isEqualTo("BooleanField");
    assertThat(strFieldMetadata.fieldType)
            .isEqualTo(FieldType.TEXT);

  }

  @Test
  public void testLuceneFieldDefMetadataEqualsHashcode() {
    String intFieldName = "intField";
    String intType = "integer";
    LuceneFieldDefMetadata intFieldMetadataA =
        new LuceneFieldDefMetadata(intFieldName, intType, true, false, false, false);
    LuceneFieldDefMetadata intFieldMetadataB =
        new LuceneFieldDefMetadata(intFieldName, intType, true, false, false, false);
    LuceneFieldDefMetadata intFieldMetadataC =
        new LuceneFieldDefMetadata("intFieldC", intType, true, false, false, false);
    LuceneFieldDefMetadata intFieldMetadataD =
        new LuceneFieldDefMetadata(intFieldName, intType, true, true, false, false);
    LuceneFieldDefMetadata intFieldMetadataE =
        new LuceneFieldDefMetadata(intFieldName, intType, true, true, false, true);

    assertThat(intFieldMetadataA).isEqualTo(intFieldMetadataB);
    assertThat(intFieldMetadataA).isNotEqualTo(intFieldMetadataC);
    assertThat(intFieldMetadataA).isNotEqualTo(intFieldMetadataD);
    assertThat(intFieldMetadataA).isNotEqualTo(intFieldMetadataE);

    assertThat(intFieldMetadataA.hashCode()).isEqualTo(intFieldMetadataB.hashCode());
    assertThat(intFieldMetadataA.hashCode()).isNotEqualTo(intFieldMetadataC.hashCode());
    assertThat(intFieldMetadataA.hashCode()).isNotEqualTo(intFieldMetadataD.hashCode());
    assertThat(intFieldMetadataA.hashCode()).isNotEqualTo(intFieldMetadataE.hashCode());
  }

  @Test
  public void invalidArgumentsShouldThrow() {
    String intFieldName = "intField";
    String intType = "integer";
    assertThatThrownBy(
            () -> new LuceneFieldDefMetadata(intFieldName, intType, true, false, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(
            () -> new LuceneFieldDefMetadata("longField", "long", true, false, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(
            () -> new LuceneFieldDefMetadata("longField", "long", true, true, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(
            () -> new LuceneFieldDefMetadata("floatField", "float", true, true, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(
            () -> new LuceneFieldDefMetadata("DoubleField", "double", true, true, true, false))
        .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(
            () -> new LuceneFieldDefMetadata("TextField", "text", true, true, true, true))
            .isInstanceOf(InvalidFieldDefException.class);

    assertThatThrownBy(
            () -> new LuceneFieldDefMetadata("BooleanField", "boolean", true, true, true, true))
            .isInstanceOf(InvalidFieldDefException.class);
  }
}
