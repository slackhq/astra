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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtil.class);

  public static Schema.IngestSchema parseSchema(Path schemaPath) throws IOException {
    String filename = schemaPath.getFileName().toString();
    try {
      String schemaFile = Files.readString(schemaPath);
      if (filename.endsWith(".yaml")) {
        return parseSchemaYaml(schemaFile, System::getenv);
      } else if (filename.endsWith(".json")) {
        return parseJsonSchema(schemaFile);
      } else {
        return Schema.IngestSchema.getDefaultInstance();
      }
    } catch (Exception e) {
      LOG.warn("Failed to read or parse schema file. Returning empty schema", e);
      return Schema.IngestSchema.getDefaultInstance();
    }
  }

  @VisibleForTesting
  public static Schema.IngestSchema parseSchemaYaml(String yamlStr, StringLookup variableResolver)
      throws JsonProcessingException, InvalidProtocolBufferException {
    StringSubstitutor substitute = new StringSubstitutor(variableResolver);
    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    ObjectMapper jsonWriter = new ObjectMapper();

    Object obj = yamlReader.readValue(substitute.replace(yamlStr), Object.class);
    return parseJsonSchema(jsonWriter.writeValueAsString(obj));
  }

  @VisibleForTesting
  public static Schema.IngestSchema parseJsonSchema(String jsonStr)
      throws InvalidProtocolBufferException {
    Schema.IngestSchema.Builder kaldbSchemaBuilder = Schema.IngestSchema.newBuilder();
    JsonFormat.parser().merge(jsonStr, kaldbSchemaBuilder);
    return kaldbSchemaBuilder.build();
  }
}
