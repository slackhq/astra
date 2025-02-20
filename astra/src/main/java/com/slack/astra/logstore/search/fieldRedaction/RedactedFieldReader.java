package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import java.io.IOException;
import java.util.HashMap;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;

/**
 * RedactedFieldReader reads in the document and creates a new RedactionStoredFieldVisitor to read
 * the individual fields. This is sometimes called by the RedactionLeafReader in
 * doGetSequentialStoredFieldsReader which is needed to fulfill the extension.
 */
class RedactedFieldReader extends StoredFieldsReader {

  private final StoredFieldsReader in;
  private final HashMap<String, FieldRedactionMetadata> fieldRedactionsMap;

  public RedactedFieldReader(
      StoredFieldsReader in, HashMap<String, FieldRedactionMetadata> fieldRedactionsMap) {
    this.in = in;
    this.fieldRedactionsMap = fieldRedactionsMap;
  }

  @Override
  public StoredFieldsReader clone() {
    return new RedactedFieldReader(in, fieldRedactionsMap);
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
    visitor = new RedactionStoredFieldVisitor(visitor, fieldRedactionsMap);
    in.document(docID, visitor);
  }
}
