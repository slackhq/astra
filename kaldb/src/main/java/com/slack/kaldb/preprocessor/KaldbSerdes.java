package com.slack.kaldb.preprocessor;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Kaldb specific serializers / deserializers. Based off of
 * org.apache.kafka.common.serialization.Serdes
 */
public class KaldbSerdes {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbSerdes.class);

  public static Serde<Murron.MurronMessage> MurronMurronMessage() {
    return new Serde<>() {
      @Override
      public Serializer<Murron.MurronMessage> serializer() {
        return (topic, data) -> {
          if (data == null) {
            return null;
          }
          return data.toByteArray();
        };
      }

      @Override
      public Deserializer<Murron.MurronMessage> deserializer() {
        return (topic, data) -> {
          Murron.MurronMessage murronMsg = null;
          if (data == null || data.length == 0) return null;

          try {
            murronMsg = Murron.MurronMessage.parseFrom(data);
          } catch (InvalidProtocolBufferException e) {
            LOG.error("Error parsing byte string into MurronMessage: {}", new String(data), e);
          }
          return murronMsg;
        };
      }
    };
  }

  public static Serde<Trace.Span> TraceSpan() {
    return new Serde<>() {
      @Override
      public Serializer<Trace.Span> serializer() {
        return (topic, data) -> {
          if (data == null) {
            return null;
          }
          return data.toByteArray();
        };
      }

      @Override
      public Deserializer<Trace.Span> deserializer() {
        return (topic, data) -> {
          Trace.Span span = null;
          if (data == null || data.length == 0) return null;

          try {
            span = Trace.Span.parseFrom(data);
          } catch (InvalidProtocolBufferException e) {
            LOG.error("Error parsing byte string into Trace.Span: {}", new String(data), e);
          }
          return span;
        };
      }
    };
  }

  public static Serde<Trace.ListOfSpans> TraceListOfSpans() {
    return new Serde<>() {
      @Override
      public Serializer<Trace.ListOfSpans> serializer() {
        return (topic, data) -> {
          if (data == null) {
            return null;
          }
          return data.toByteArray();
        };
      }

      @Override
      public Deserializer<Trace.ListOfSpans> deserializer() {
        return (topic, data) -> {
          Trace.ListOfSpans listOfSpans = null;
          if (data == null || data.length == 0) return null;

          try {
            listOfSpans = Trace.ListOfSpans.parseFrom(data);
          } catch (InvalidProtocolBufferException e) {
            LOG.error("Error parsing byte string into Trace.ListOfSpans: {}", new String(data), e);
          }
          return listOfSpans;
        };
      }
    };
  }
}
