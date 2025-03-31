package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import java.io.IOException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;

/**
 * RedactionFilterDirectoryReader is Astra's FilterDirectoryReader and creates a
 * RedactionSubReaderWrapper to read in the data.
 *
 * <p>filter reader > sub reader wrapper > leaf reader > stored field reader, which uses the stored
 * field visitor to perform the field level redactions Heavily inspired by the
 * DlsFlsFilterLeafReader from opensearch
 * https://github.com/opensearch-project/security/blob/4f2e689a37765786dd256a7591434815bbb950a4/src/main/java/org/opensearch/security/configuration/DlsFlsFilterLeafReader.java
 */
public class RedactionFilterDirectoryReader extends FilterDirectoryReader {
  public final FieldRedactionMetadataStore fieldRedactionMetadataStore;

  public RedactionFilterDirectoryReader(
      DirectoryReader in, FieldRedactionMetadataStore fieldRedactionMetadataStore)
      throws IOException {
    super(in, new RedactionSubReaderWrapper());
    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
  }

  public RedactionFilterDirectoryReader(
      IndexWriter indexWriter, FieldRedactionMetadataStore fieldRedactionMetadataStore)
      throws IOException {
    super(DirectoryReader.open(indexWriter), new RedactionSubReaderWrapper());
    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
    return new RedactionFilterDirectoryReader(in, fieldRedactionMetadataStore);
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  @Override
  protected void doClose() throws IOException {
    super.doClose();
  }
}
