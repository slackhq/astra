package com.slack.kaldb.logstore.columnar.io.impl;

import static com.slack.kaldb.testlib.SpanUtil.makeSpan;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.io.Files;
import com.slack.kaldb.logstore.columnar.FileReader;
import com.slack.kaldb.logstore.columnar.FileWriter;
import com.slack.kaldb.logstore.columnar.KeyValue;
import com.slack.kaldb.logstore.columnar.common.LogFilePath;
import com.slack.kaldb.logstore.columnar.common.SecorConfig;
import com.slack.kaldb.logstore.columnar.impl.ProtobufParquetFileReaderWriterFactory;
import com.slack.kaldb.logstore.columnar.util.ParquetUtil;
import com.slack.service.murron.trace.Trace;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufParquetTest {
    private static final Logger LOG = LoggerFactory.getLogger(ProtobufParquetTest.class);

   private SecorConfig config;
  private LogFilePath tempLogFilePath;

  @Before
  public void setUp() {
    config = spy(new SecorConfig(new PropertiesConfiguration()));

    // PropertiesConfiguration properties = new PropertiesConfiguration();
    Map<String, String> classPerTopic = new HashMap<String, String>();
    classPerTopic.put("test-pb-topic", Trace.Span.class.getName());
    when(config.getProtobufMessageClassPerTopic()).thenReturn(classPerTopic);
    when(ParquetUtil.getParquetBlockSize(config)).thenReturn(ParquetWriter.DEFAULT_BLOCK_SIZE);
    when(ParquetUtil.getParquetPageSize(config)).thenReturn(ParquetWriter.DEFAULT_PAGE_SIZE);
    when(ParquetUtil.getParquetEnableDictionary(config))
        .thenReturn(ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED);
    when(ParquetUtil.getParquetValidation(config))
        .thenReturn(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED);

    String tempFilePath = Files.createTempDir().toString();
    LOG.info("Temp file path is: {}", tempFilePath);
    tempLogFilePath =
        new LogFilePath(
            tempFilePath, "test-pb-topic", new String[] {"part-1"}, 0, 1, 23232, ".log");
  }

  @After
  public void tearDown() {}

  @Test
  public void testWriterReader() throws Exception {
    ProtobufParquetFileReaderWriterFactory factory =
        new ProtobufParquetFileReaderWriterFactory(config);
    FileWriter writer = factory.BuildFileWriter(tempLogFilePath, null);

    final String traceId = "t1";
    final long timestampMicros = 1612550512340953L;
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "test_span";
    final String msgType = "msg_type";
    final Trace.Span span1 =
        makeSpan(traceId, "1", "0", timestampMicros, durationMicros, name, serviceName, msgType);
    final Trace.Span span2 =
        makeSpan(
            traceId, "2", "1", timestampMicros + 1, durationMicros + 1, name, serviceName, msgType);

    KeyValue kv1 = new KeyValue(23232L, span1.toByteArray());
    KeyValue kv2 = new KeyValue(23233L, span2.toByteArray());
    writer.write(kv1);
    writer.write(kv2);
    writer.close();

    FileReader fileReader = factory.BuildFileReader(tempLogFilePath, null);

    KeyValue kvOut1 = fileReader.next();
    assertThat(kvOut1.getOffset()).isEqualTo(kv1.getOffset());
    assertThat(Trace.Span.parseFrom(kvOut1.getValue())).isEqualTo(span1);

    KeyValue kvOut2 = fileReader.next();
    assertThat(kvOut2.getOffset()).isEqualTo(kv2.getOffset());
    assertThat(Trace.Span.parseFrom(kvOut2.getValue())).isEqualTo(span2);
    fileReader.close();
  }
}
