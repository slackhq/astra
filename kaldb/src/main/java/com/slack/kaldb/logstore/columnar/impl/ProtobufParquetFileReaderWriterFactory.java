package com.slack.kaldb.logstore.columnar.impl;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.slack.kaldb.logstore.columnar.FileReader;
import com.slack.kaldb.logstore.columnar.FileReaderWriterFactory;
import com.slack.kaldb.logstore.columnar.FileWriter;
import com.slack.kaldb.logstore.columnar.KeyValue;
import com.slack.kaldb.logstore.columnar.common.LogFilePath;
import com.slack.kaldb.logstore.columnar.common.SecorConfig;
import com.slack.kaldb.logstore.columnar.util.ParquetUtil;
import com.slack.kaldb.logstore.columnar.util.ProtobufUtil;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoParquetWriter;

/**
 * Implementation for reading/writing protobuf messages to/from Parquet files.
 *
 * @author Michael Spector (spektom@gmail.com)
 */
public class ProtobufParquetFileReaderWriterFactory implements FileReaderWriterFactory {

  private ProtobufUtil protobufUtil;

  protected final int blockSize;
  protected final int pageSize;
  protected final boolean enableDictionary;
  protected final boolean validating;

  public ProtobufParquetFileReaderWriterFactory(SecorConfig config) {
    protobufUtil = new ProtobufUtil(config);

    blockSize = ParquetUtil.getParquetBlockSize(config);
    pageSize = ParquetUtil.getParquetPageSize(config);
    enableDictionary = ParquetUtil.getParquetEnableDictionary(config);
    validating = ParquetUtil.getParquetValidation(config);
  }

  @Override
  public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec)
      throws Exception {
    return new ProtobufParquetFileReader(logFilePath, codec);
  }

  @Override
  public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec)
      throws Exception {
    return new ProtobufParquetFileWriter(logFilePath, codec);
  }

  protected class ProtobufParquetFileReader implements FileReader {

    private ParquetReader<MessageOrBuilder> reader;
    private long offset;

    public ProtobufParquetFileReader(LogFilePath logFilePath, CompressionCodec codec)
        throws IOException {
      Path path = new Path(logFilePath.getLogFilePath());
      reader = ProtoParquetReader.<MessageOrBuilder>builder(path).build();
      offset = logFilePath.getOffset();
    }

    @Override
    public KeyValue next() throws IOException {
      Builder messageBuilder = (Builder) reader.read();
      if (messageBuilder != null) {
        return new KeyValue(offset++, messageBuilder.build().toByteArray());
      }
      return null;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  protected class ProtobufParquetFileWriter implements FileWriter {

    private ProtoParquetWriter<Message> writer;
    private String topic;

    public ProtobufParquetFileWriter(LogFilePath logFilePath, CompressionCodec codec)
        throws IOException {
      Path path = new Path(logFilePath.getLogFilePath());
      CompressionCodecName codecName =
          CompressionCodecName.fromCompressionCodec(codec != null ? codec.getClass() : null);
      topic = logFilePath.getTopic();
      writer =
          new ProtoParquetWriter<Message>(
              path,
              protobufUtil.getMessageClass(topic),
              codecName,
              blockSize,
              pageSize,
              enableDictionary,
              validating);
    }

    @Override
    public long getLength() throws IOException {
      return writer.getDataSize();
    }

    @Override
    public void write(KeyValue keyValue) throws IOException {
      Message message = protobufUtil.decodeProtobufOrJsonMessage(topic, keyValue.getValue());
      writer.write(message);
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }
}
