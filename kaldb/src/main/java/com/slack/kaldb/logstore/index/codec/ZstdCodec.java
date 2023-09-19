package com.slack.kaldb.logstore.index.codec;

import java.io.IOException;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

// Inspired by https://github.com/opensearch-project/custom-codecs/
// We can reuse that and remove this once custom-codecs has a release
// Update this when updating lucene versions
public final class ZstdCodec extends FilterCodec {

  private final ZstdStoredFieldsFormat storedFieldsFormat;

  public ZstdCodec() {
    super("CustomCodec", new Lucene95Codec());
    this.storedFieldsFormat = new ZstdStoredFieldsFormat();
  }

  public StoredFieldsFormat storedFieldsFormat() {
    return storedFieldsFormat;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  class ZstdStoredFieldsFormat extends StoredFieldsFormat {

    private static final int ZSTD_BLOCK_LENGTH = 10 * 48 * 1024;
    private static final int ZSTD_MAX_DOCS_PER_BLOCK = 4096;
    private static final int ZSTD_BLOCK_SHIFT = 10;
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;

    private final CompressionMode zstdCompressionMode;

    private ZstdStoredFieldsFormat() {
      zstdCompressionMode = new ZstdCompressionMode(DEFAULT_COMPRESSION_LEVEL);
    }

    @Override
    public StoredFieldsReader fieldsReader(
        Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
      Lucene90CompressingStoredFieldsFormat impl =
          new Lucene90CompressingStoredFieldsFormat(
              "CustomStoredFieldsZstd",
              zstdCompressionMode,
              ZSTD_BLOCK_LENGTH,
              ZSTD_MAX_DOCS_PER_BLOCK,
              ZSTD_BLOCK_SHIFT);
      return impl.fieldsReader(directory, si, fn, context);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context)
        throws IOException {
      Lucene90CompressingStoredFieldsFormat impl =
          new Lucene90CompressingStoredFieldsFormat(
              "CustomStoredFieldsZstd",
              zstdCompressionMode,
              ZSTD_BLOCK_LENGTH,
              ZSTD_MAX_DOCS_PER_BLOCK,
              ZSTD_BLOCK_SHIFT);
      return impl.fieldsWriter(directory, si, context);
    }
  }
}
