package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import java.io.IOException;
import java.util.HashMap;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;

/**
 * RedactionLeafReader is Astra's leaf reader and creates a StoredFieldsReader or a
 * RedactionStoredFieldVisitor to read in the individual fields.
 */
class RedactionLeafReader extends SequentialStoredFieldsLeafReader {
  private final HashMap<String, FieldRedactionMetadata> fieldRedactionsMap;

  public RedactionLeafReader(
      LeafReader in, HashMap<String, FieldRedactionMetadata> fieldRedactionsMap) {
    super(in);
    this.fieldRedactionsMap = fieldRedactionsMap;
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    return in.getSortedDocValues(field);
  }

  @Override
  public StoredFields storedFields() throws IOException {
    return in.storedFields();
  }

  // RedactionStoredFieldVisitor can be called here or in the RedactedFieldReader
  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    visitor = new RedactionStoredFieldVisitor(visitor, fieldRedactionsMap);
    in.document(docID, visitor);
  }

  @Override
  protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
    return new RedactedFieldReader(reader, fieldRedactionsMap);
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return in.getCoreCacheHelper();
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
