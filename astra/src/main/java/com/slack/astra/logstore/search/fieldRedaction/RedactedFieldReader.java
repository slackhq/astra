package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;
import java.util.List;

// implements the redacted field reader
class RedactedFieldReader extends StoredFieldsReader {

    private final StoredFieldsReader in;
    private final List<FieldRedactionMetadata> fieldRedactions;

    public RedactedFieldReader(
            StoredFieldsReader in, List<FieldRedactionMetadata> fieldRedactions) {
        this.in = in;
        this.fieldRedactions = fieldRedactions;
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

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        //        visitor = getDlsFlsVisitor(visitor);
        try {
            visitor = new RedactionStoredFieldVisitor(visitor, fieldRedactions);
            in.document(docID, visitor);
        } finally {
            //          finishVisitor(visitor);
        }
    }
}
