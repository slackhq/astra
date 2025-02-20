package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import java.util.HashMap;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;

/**
 * RedactionSubReaderWrapper is called by the RedactionFilterDirectoryReader as a wrapper to the
 * reader, and creates a RedactionLeafReader.
 */
class RedactionSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
  private final HashMap<String, FieldRedactionMetadata> fieldRedactionsMap;

  public RedactionSubReaderWrapper(FieldRedactionMetadataStore fieldRedactionMetadataStore) {
    this.fieldRedactionsMap = new HashMap<>();
    if (fieldRedactionMetadataStore != null) {
      fieldRedactionMetadataStore
          .listSync()
          .forEach(
              redaction -> {
                fieldRedactionsMap.put(redaction.getName(), redaction);
              });
    }
  }

  @Override
  public LeafReader wrap(LeafReader reader) {
    return new RedactionLeafReader(reader, this.fieldRedactionsMap);
  }
}
