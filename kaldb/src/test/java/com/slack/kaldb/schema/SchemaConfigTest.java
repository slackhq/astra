package com.slack.kaldb.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.slack.kaldb.metadata.schema.SchemaUtil;
import com.slack.kaldb.proto.schema.Schema;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SchemaConfigTest {

  @Test
  public void testParsingEmptySchema() {
    List.of("json", "yaml")
        .forEach(
            ext -> {
              try {
                final File cfgFile =
                    new File(
                        getClass()
                            .getClassLoader()
                            .getResource("schema/test_schema_empty." + ext)
                            .getFile());
                Schema.PreprocessorSchema schema = SchemaUtil.parseSchema(cfgFile.toPath());
                assertThat(schema).isNotNull();
                assertThat(schema.getFieldsCount()).isEqualTo(0);
              } catch (IOException e) {
                fail("Failed to parse schema", e);
              }
            });
  }

  @Test
  public void testParseSchemaInvalidFile() throws IOException {
    Path invalidPath = Path.of("invalid_path.yaml");
    Schema.PreprocessorSchema schema = SchemaUtil.parseSchema(invalidPath);
    assertThat(schema).isNotNull();
    assertThat(schema.getFieldsCount()).isEqualTo(0);
  }

  @Test
  public void testParsingSchema() {
    List.of("json", "yaml")
        .forEach(
            ext -> {
              try {
                final File cfgFile =
                    new File(
                        getClass()
                            .getClassLoader()
                            .getResource("schema/test_schema." + ext)
                            .getFile());
                Schema.PreprocessorSchema schema = SchemaUtil.parseSchema(cfgFile.toPath());
                assertThat(schema).isNotNull();

                assertThat(schema).isNotNull();
                assertThat(schema.getFieldsCount()).isEqualTo(13);

                assertThat(schema.getFieldsMap().get("host").getType())
                    .isEqualTo(Schema.SchemaFieldType.KEYWORD);

                assertThat(schema.getFieldsMap().get("message").getType())
                    .isEqualTo(Schema.SchemaFieldType.TEXT);

                assertThat(schema.getFieldsMap().get("ip").getType())
                    .isEqualTo(Schema.SchemaFieldType.IP);

                assertThat(schema.getFieldsMap().get("timestamp").getType())
                    .isEqualTo(Schema.SchemaFieldType.DATE);

                assertThat(schema.getFieldsMap().get("success").getType())
                    .isEqualTo(Schema.SchemaFieldType.BOOLEAN);

                assertThat(schema.getFieldsMap().get("cost").getType())
                    .isEqualTo(Schema.SchemaFieldType.DOUBLE);

                assertThat(schema.getFieldsMap().get("amount").getType())
                    .isEqualTo(Schema.SchemaFieldType.FLOAT);

                assertThat(schema.getFieldsMap().get("amount_half_float").getType())
                    .isEqualTo(Schema.SchemaFieldType.HALF_FLOAT);

                assertThat(schema.getFieldsMap().get("value").getType())
                    .isEqualTo(Schema.SchemaFieldType.INTEGER);

                assertThat(schema.getFieldsMap().get("count").getType())
                    .isEqualTo(Schema.SchemaFieldType.LONG);

                assertThat(schema.getFieldsMap().get("count_scaled_long").getType())
                    .isEqualTo(Schema.SchemaFieldType.SCALED_LONG);

                assertThat(schema.getFieldsMap().get("count_short").getType())
                    .isEqualTo(Schema.SchemaFieldType.SHORT);

                assertThat(schema.getFieldsMap().get("bucket").getType())
                    .isEqualTo(Schema.SchemaFieldType.BYTE);
              } catch (IOException e) {
                fail("Failed to parse schema", e);
              }
            });
  }
}
