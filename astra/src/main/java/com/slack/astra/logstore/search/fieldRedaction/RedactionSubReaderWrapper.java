package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;

import java.io.IOException;
import java.util.List;

// Implements a field redaction subreaderwrapper
class RedactionSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
    private final List<FieldRedactionMetadata> redactedFields;

    public RedactionSubReaderWrapper(List<FieldRedactionMetadata> redactedFields)
            throws IOException {
        this.redactedFields = redactedFields;
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
        return new RedactionLeafReader(reader, this.redactedFields);
    }
}
