package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import java.io.IOException;
import java.util.Map;

// Implements a field redaction subreaderwrapper
class RedactionSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
    private final Map<String, FieldRedactionMetadata> redactedFields;

    public RedactionSubReaderWrapper(Map<String, FieldRedactionMetadata> redactedFieldsMap)
            throws IOException {
        this.redactedFields = redactedFieldsMap;
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
        return new RedactionLeafReader(reader, this.redactedFields);
    }
}
