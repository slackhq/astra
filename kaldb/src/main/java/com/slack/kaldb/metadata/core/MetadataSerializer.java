package com.slack.kaldb.metadata.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

/**
 * An interface that helps us covert protobuf objects to and from json.
 *
 * <p>This class also includes a base printer and parser for serializing and de-serializing the
 * protobuf objects to and from Json.
 */
public interface MetadataSerializer<T extends KaldbMetadata> {
  // TODO: Print enum as ints instead of Strings also?
  JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();
  JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();

  String toJsonStr(T metadata) throws InvalidProtocolBufferException;

  T fromJsonStr(String data) throws InvalidProtocolBufferException;
}
