package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import java.io.IOException;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;

// implements the redacted field reader
class RedactedFieldReader extends StoredFieldsReader {

  private final StoredFieldsReader in;
  private final FieldRedactionMetadataStore fieldRedactionMetadataStore;

  public RedactedFieldReader(
      StoredFieldsReader in, FieldRedactionMetadataStore fieldRedactionMetadataStore) {
    this.in = in;
    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
  }

  @Override
  public StoredFieldsReader clone() {
    return new RedactedFieldReader(in, fieldRedactionMetadataStore);
  }

  @Override
  public void checkIntegrity() throws IOException {
    in.checkIntegrity();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    visitor = new RedactionStoredFieldVisitor(visitor, fieldRedactionMetadataStore);
    in.document(docID, visitor);
  }
}
