package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import java.io.IOException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;

// filter reader > sub reader wrapper > leaf reader > stored field reader,
//   which uses the stored field visitor to perform the field level redactions
// Heavily inspired by the DlsFlsFilterLeafReader from opensearch
// https://github.com/opensearch-project/security/blob/4f2e689a37765786dd256a7591434815bbb950a4/src/main/java/org/opensearch/security/configuration/DlsFlsFilterLeafReader.java

// Implements a filterdirectoryreader for field redaction
public class RedactionFilterDirectoryReader extends FilterDirectoryReader {
  private final FieldRedactionMetadataStore fieldRedactionMetadataStore;

  public RedactionFilterDirectoryReader(
      DirectoryReader in, FieldRedactionMetadataStore fieldRedactionMetadataStore)
      throws IOException {
    super(in, new RedactionSubReaderWrapper(fieldRedactionMetadataStore));
    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
  }

  public RedactionFilterDirectoryReader(
      IndexWriter indexWriter, FieldRedactionMetadataStore fieldRedactionMetadataStore)
      throws IOException {
    super(
        DirectoryReader.open(indexWriter),
        new RedactionSubReaderWrapper(fieldRedactionMetadataStore));
    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
    return new RedactionFilterDirectoryReader(in, fieldRedactionMetadataStore);
  }

  /* from FilterDirectoryReader - this should call our doWrapDirectoryReader which should do what we want with RedactionFilterDirectoryReader
      @Override
  protected final DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes)
      throws IOException {
    return wrapDirectoryReader(in.doOpenIfChanged(writer, applyAllDeletes));
  }

    private final DirectoryReader wrapDirectoryReader(DirectoryReader in) throws IOException {
    return in == null ? null : doWrapDirectoryReader(in);
  }
     */

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  @Override
  protected void doClose() throws IOException {
    super.doClose();
    // todo - not sure if this close is correct
    this.fieldRedactionMetadataStore.close();
  }
}
