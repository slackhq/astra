package com.slack.kaldb.metadata.schema;

import static com.slack.kaldb.metadata.schema.FieldType.convertFieldValue;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class FieldTypeTest {
  @Test
  public void testValueTypeConversionWorks() {
    assertThat(convertFieldValue("1", FieldType.TEXT, FieldType.INTEGER)).isEqualTo(1);
    assertThat(convertFieldValue("1", FieldType.TEXT, FieldType.LONG)).isEqualTo(1L);
    assertThat(convertFieldValue("2", FieldType.TEXT, FieldType.FLOAT)).isEqualTo(2.0f);
    assertThat(convertFieldValue("3", FieldType.TEXT, FieldType.DOUBLE)).isEqualTo(3.0d);
    assertThat(convertFieldValue("4", FieldType.TEXT, FieldType.STRING)).isEqualTo("4");
    assertThat(convertFieldValue("4", FieldType.STRING, FieldType.TEXT)).isEqualTo("4");

    int intValue = 1;
    assertThat(convertFieldValue(intValue, FieldType.INTEGER, FieldType.TEXT)).isEqualTo("1");
    assertThat(convertFieldValue(intValue, FieldType.INTEGER, FieldType.STRING)).isEqualTo("1");
    assertThat(convertFieldValue(intValue + 1, FieldType.INTEGER, FieldType.LONG)).isEqualTo(2L);
    assertThat(convertFieldValue(intValue + 2, FieldType.INTEGER, FieldType.FLOAT)).isEqualTo(3.0f);
    assertThat(convertFieldValue(intValue + 3, FieldType.INTEGER, FieldType.DOUBLE))
        .isEqualTo(4.0d);

    long longValue = 1L;
    assertThat(convertFieldValue(longValue, FieldType.LONG, FieldType.TEXT)).isEqualTo("1");
    assertThat(convertFieldValue(longValue, FieldType.LONG, FieldType.STRING)).isEqualTo("1");
    assertThat(convertFieldValue(longValue + 1, FieldType.LONG, FieldType.INTEGER)).isEqualTo(2);
    assertThat(convertFieldValue(longValue + 2, FieldType.LONG, FieldType.FLOAT)).isEqualTo(3.0f);
    assertThat(convertFieldValue(longValue + 3, FieldType.LONG, FieldType.DOUBLE)).isEqualTo(4.0);

    float floatValue = 1.0f;
    assertThat(convertFieldValue(floatValue, FieldType.FLOAT, FieldType.TEXT)).isEqualTo("1.0");
    assertThat(convertFieldValue(floatValue, FieldType.FLOAT, FieldType.STRING)).isEqualTo("1.0");
    assertThat(convertFieldValue(floatValue + 1.0f, FieldType.FLOAT, FieldType.INTEGER))
        .isEqualTo(2);
    assertThat(convertFieldValue(floatValue + 2.0f, FieldType.FLOAT, FieldType.LONG)).isEqualTo(3L);
    assertThat(convertFieldValue(floatValue + 3.0f, FieldType.FLOAT, FieldType.DOUBLE))
        .isEqualTo(4.0);

    double doubleValue = 1.0;
    assertThat(convertFieldValue(doubleValue, FieldType.DOUBLE, FieldType.TEXT)).isEqualTo("1.0");
    assertThat(convertFieldValue(doubleValue, FieldType.DOUBLE, FieldType.STRING)).isEqualTo("1.0");
    assertThat(convertFieldValue(doubleValue + 1.0f, FieldType.DOUBLE, FieldType.INTEGER))
        .isEqualTo(2);
    assertThat(convertFieldValue(doubleValue + 2.0f, FieldType.DOUBLE, FieldType.LONG))
        .isEqualTo(3L);
    assertThat(convertFieldValue(doubleValue + 3.0f, FieldType.DOUBLE, FieldType.FLOAT))
        .isEqualTo(4.0f);

    // Test conversion failures
    assertThat(convertFieldValue("testStr1", FieldType.TEXT, FieldType.INTEGER)).isEqualTo(0);
    assertThat(convertFieldValue("testStr2", FieldType.TEXT, FieldType.LONG)).isEqualTo(0L);
    assertThat(convertFieldValue("testStr3", FieldType.TEXT, FieldType.FLOAT)).isEqualTo(0f);
    assertThat(convertFieldValue("testStr4", FieldType.TEXT, FieldType.DOUBLE)).isEqualTo(0d);

    // Max values of the types, causes loss of precision in some cases but not failures.
    long longMaxValue = Long.MAX_VALUE;
    assertThat(convertFieldValue(longMaxValue, FieldType.LONG, FieldType.TEXT))
        .isEqualTo(Long.valueOf(longMaxValue).toString());
    assertThat(convertFieldValue(longMaxValue, FieldType.LONG, FieldType.STRING))
        .isEqualTo(Long.valueOf(longMaxValue).toString());
    assertThat(convertFieldValue(longMaxValue, FieldType.LONG, FieldType.INTEGER)).isNotNull();
    assertThat(convertFieldValue(longMaxValue, FieldType.LONG, FieldType.FLOAT)).isNotNull();
    assertThat(convertFieldValue(longMaxValue, FieldType.LONG, FieldType.DOUBLE)).isNotNull();

    float floatMaxValue = Float.MAX_VALUE;
    assertThat(convertFieldValue(floatMaxValue, FieldType.FLOAT, FieldType.TEXT))
        .isEqualTo(Float.valueOf(floatMaxValue).toString());
    assertThat(convertFieldValue(floatMaxValue, FieldType.FLOAT, FieldType.STRING))
        .isEqualTo(Float.valueOf(floatMaxValue).toString());
    assertThat(convertFieldValue(floatMaxValue, FieldType.FLOAT, FieldType.INTEGER)).isNotNull();
    assertThat(convertFieldValue(floatMaxValue, FieldType.FLOAT, FieldType.LONG)).isNotNull();
    assertThat(convertFieldValue(floatMaxValue, FieldType.FLOAT, FieldType.DOUBLE)).isNotNull();

    double doubleMaxValue = Double.MAX_VALUE;
    assertThat(convertFieldValue(doubleMaxValue, FieldType.DOUBLE, FieldType.TEXT))
        .isEqualTo(Double.valueOf(doubleMaxValue).toString());
    assertThat(convertFieldValue(doubleMaxValue, FieldType.DOUBLE, FieldType.STRING))
        .isEqualTo(Double.valueOf(doubleMaxValue).toString());
    assertThat(convertFieldValue(doubleMaxValue, FieldType.DOUBLE, FieldType.INTEGER)).isNotNull();
    assertThat(convertFieldValue(doubleMaxValue, FieldType.DOUBLE, FieldType.LONG)).isNotNull();
    assertThat(convertFieldValue(doubleMaxValue, FieldType.DOUBLE, FieldType.FLOAT)).isNotNull();

    // Convert same type
    assertThat(convertFieldValue("1", FieldType.TEXT, FieldType.TEXT)).isEqualTo("1");
    assertThat(convertFieldValue("1", FieldType.STRING, FieldType.STRING)).isEqualTo("1");
    assertThat(convertFieldValue(1L, FieldType.LONG, FieldType.LONG)).isEqualTo(1L);
    assertThat(convertFieldValue(2.0f, FieldType.FLOAT, FieldType.FLOAT)).isEqualTo(2.0f);
    assertThat(convertFieldValue(3.0d, FieldType.DOUBLE, FieldType.DOUBLE)).isEqualTo(3.0d);
  }
}
