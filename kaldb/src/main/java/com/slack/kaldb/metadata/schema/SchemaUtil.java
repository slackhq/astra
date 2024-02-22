package com.slack.kaldb.metadata.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.proto.schema.Schema;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;

public class SchemaUtil {

  public static Schema.PreprocessorSchema parseSchema(Path schemaPath) throws IOException {
    String filename = schemaPath.getFileName().toString();
    if (filename.endsWith(".yaml")) {
      return parseSchemaYaml(Files.readString(schemaPath), System::getenv);
    } else if (filename.endsWith(".json")) {
      return parseJsonSchema(Files.readString(schemaPath));
    } else {
      throw new RuntimeException(
          "Invalid config file format provided - must be either .json or .yaml");
    }
  }

  @VisibleForTesting
  public static Schema.PreprocessorSchema parseSchemaYaml(
      String yamlStr, StringLookup variableResolver)
      throws JsonProcessingException, InvalidProtocolBufferException {
    StringSubstitutor substitute = new StringSubstitutor(variableResolver);
    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    ObjectMapper jsonWriter = new ObjectMapper();

    Object obj = yamlReader.readValue(substitute.replace(yamlStr), Object.class);
    return parseJsonSchema(jsonWriter.writeValueAsString(obj));
  }

  @VisibleForTesting
  public static Schema.PreprocessorSchema parseJsonSchema(String jsonStr)
      throws InvalidProtocolBufferException {
    Schema.PreprocessorSchema.Builder kaldbSchemaBuilder = Schema.PreprocessorSchema.newBuilder();
    JsonFormat.parser().merge(jsonStr, kaldbSchemaBuilder);
    Schema.PreprocessorSchema kaldbSchema = kaldbSchemaBuilder.build();
    // TODO: validate schema
    return kaldbSchema;
  }
}
