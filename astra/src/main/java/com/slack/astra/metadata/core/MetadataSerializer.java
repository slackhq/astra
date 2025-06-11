package com.slack.astra.metadata.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.curator.x.async.modeled.ModelSerializer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An interface that helps us covert protobuf objects to and from json.
 *
 * <p>This class also includes a base printer and parser for serializing and de-serializing the
 * protobuf objects to and from Json.
 */
public interface MetadataSerializer<T extends AstraMetadata> {
  // TODO: Print enum as ints instead of Strings also?
  JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();
  JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();

  String toJsonStr(T metadata) throws InvalidProtocolBufferException;

  T fromJsonStr(String data) throws InvalidProtocolBufferException;

  default ModelSerializer<T> toModelSerializer() {
    return new ModelSerializer<>() {
      @Override
      public byte[] serialize(T model) {
        if (model == null) {
          return null;
        }

        try {
          return toJsonStr(model).getBytes(UTF_8);
        } catch (Exception e) {
          throw new IllegalArgumentException(e);
        }
      }

      @Override
      public T deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
          return null;
        }

        try {
          return fromJsonStr(new String(bytes, UTF_8));
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    };
  }
}
