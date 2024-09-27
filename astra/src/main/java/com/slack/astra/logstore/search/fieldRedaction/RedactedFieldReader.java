package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

// implements the redacted field reader
class RedactedFieldReader extends StoredFieldsReader {

    private final StoredFieldsReader in;
    private final Map<String, Object> fieldRedactionsMap;

    public RedactedFieldReader(
            StoredFieldsReader in, Map<String, Object> fieldRedactionsMap) {
        this.in = in;
//        this.fieldRedactions = fieldRedactions;

        this.fieldRedactionsMap = fieldRedactionsMap;
    }

    @Override
    public StoredFieldsReader clone() {
        return new RedactedFieldReader(in, fieldRedactions);
    }

    @Override
    public void checkIntegrity() throws IOException {
        in.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    // todo - Do we need a field visitor on the field reader AND the leaf reader?
    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        //        visitor = getDlsFlsVisitor(visitor);
//        try {
            visitor = new RedactionStoredFieldVisitor(visitor, fieldRedactions);
            in.document(docID, visitor);
//        } finally {
            //          finishVisitor(visitor);
//        }
    }
}
