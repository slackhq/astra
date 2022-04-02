package com.slack.kaldb.logstore.columnar;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.logstore.Message;
import com.slack.kaldb.logstore.columnar.common.LogFilePath;
import com.slack.kaldb.logstore.columnar.common.SecorConfig;
import com.slack.kaldb.logstore.columnar.impl.ProtobufParquetFileReaderWriterFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.SearcherManager;

public class ParquetIndexStoreImpl implements LogStore<LogMessage> {

  private final FileWriter writer;
  private final File dataDirectory;

  public ParquetIndexStoreImpl(File dataDirectory) throws Exception {
    this.dataDirectory = dataDirectory;
    SecorConfig config = new SecorConfig(new PropertiesConfiguration());
    ProtobufParquetFileReaderWriterFactory factory =
        new ProtobufParquetFileReaderWriterFactory(config);

    LogFilePath logFilePath =
        new LogFilePath(
            dataDirectory.getAbsolutePath(),
            "test-pb-topic",
            new String[] {"part-1"},
            0,
            1,
            23232,
            ".log");

    writer = factory.BuildFileWriter(logFilePath, null);
  }

  @Override
  public void addMessage(LogMessage message) {
    Message protoMessage = toProtoSpanMessage(message);
    KeyValue kv1 = new KeyValue(23232L, protoMessage.toString().getBytes());
    try {
      writer.write(kv1);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write message:" + message, e);
    }
  }

  private Message toProtoSpanMessage(LogMessage message) {
    return null;
  }

  @Override
  public SearcherManager getSearcherManager() {
    return null;
  }

  @Override
  public void commit() {}

  @Override
  public void refresh() {}

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public void cleanup() throws IOException {}

  @Override
  public Path getDirectory() {
    return dataDirectory.toPath();
  }

  @Override
  public IndexCommit getIndexCommit() {
    return null;
  }

  @Override
  public IndexWriter getIndexWriter() {
    return null;
  }

  @Override
  public void releaseIndexCommit(IndexCommit indexCommit) {}

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
