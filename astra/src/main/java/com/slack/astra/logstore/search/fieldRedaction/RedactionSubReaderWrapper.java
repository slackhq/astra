package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import java.io.IOException;

// Implements a field redaction subreaderwrapper
class RedactionSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
    private final FieldRedactionMetadataStore fieldRedactionMetadataStore;

    public RedactionSubReaderWrapper(FieldRedactionMetadataStore fieldRedactionMetadataStore)
            throws IOException {
        this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
        return new RedactionLeafReader(reader, this.fieldRedactionMetadataStore);
    }
}
