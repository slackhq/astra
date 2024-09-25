package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;
import java.util.List;

// Implements the redaction leaf reader
class RedactionLeafReader extends SequentialStoredFieldsLeafReader {
    private final List<FieldRedactionMetadata> fieldRedactions;

    public RedactionLeafReader(LeafReader in, List<FieldRedactionMetadata> fieldRedactions) {
        super(in);
        this.fieldRedactions = fieldRedactions;
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        return in.getSortedDocValues(field);
    }

    @Override
    public StoredFields storedFields() throws IOException {
        return in.storedFields();
    }

    // todo is this correct? Why would a field visitor be on both a leaf reader and a field
    // reader?
    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        visitor = new RedactionStoredFieldVisitor(visitor, fieldRedactions);
        in.document(docID, visitor);
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return new RedactedFieldReader(reader, fieldRedactions);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
