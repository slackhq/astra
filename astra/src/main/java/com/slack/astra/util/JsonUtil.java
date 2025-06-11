package com.slack.astra.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import java.io.IOException;
import java.nio.ByteBuffer;

public class JsonUtil {
  private static JsonUtil ourInstance = new JsonUtil();
  private final ObjectMapper mapper;

  public static JsonUtil getInstance() {
    return ourInstance;
  }

  public static <T> ByteBuffer toByteBuffer(T obj) throws JsonProcessingException {
    return ByteBuffer.wrap(writeAsString(obj).getBytes(UTF_8));
  }

  public static <T> String writeAsString(T obj) throws JsonProcessingException {
    return ourInstance.mapper.writeValueAsString(obj);
  }

  // TODO: Ensure this class templating works here.
  public static <T> T read(String s, Class<T> cls) throws IOException {
    return ourInstance.mapper.readValue(s, cls);
  }

  public static <T> T read(String s, TypeReference<T> valueTypeRef) throws JsonProcessingException {
    return ourInstance.mapper.readValue(s, valueTypeRef);
  }

  public static <T> T read(byte[] data, TypeReference<T> valueTypeRef) throws IOException {
    return ourInstance.mapper.readValue(data, valueTypeRef);
  }

  private JsonUtil() {
    mapper =
        JsonMapper.builder()
            .addModule(new AfterburnerModule())
            .addModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
            .build();
  }
}
