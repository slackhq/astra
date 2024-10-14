package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;

class RedactionSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
  private final FieldRedactionMetadataStore fieldRedactionMetadataStore;

  public RedactionSubReaderWrapper(FieldRedactionMetadataStore fieldRedactionMetadataStore) {
    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
  }

  @Override
  public LeafReader wrap(LeafReader reader) {
    return new RedactionLeafReader(reader, this.fieldRedactionMetadataStore);
  }
}
