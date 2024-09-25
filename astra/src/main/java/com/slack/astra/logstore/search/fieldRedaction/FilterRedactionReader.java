package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;

import java.io.IOException;
import java.util.List;

// filter reader > sub reader wrapper > leaf reader > stored field reader,
//   which uses the stored field visitor to perform the field level redactions
// Heavily inspired by the DlsFlsFilterLeafReader from opensearch
// https://github.com/opensearch-project/security/blob/4f2e689a37765786dd256a7591434815bbb950a4/src/main/java/org/opensearch/security/configuration/DlsFlsFilterLeafReader.java

// Implements a filterdirectoryreader for field redaction
public class FilterRedactionReader extends FilterDirectoryReader {
    private final List<FieldRedactionMetadata> fieldRedactions;

    public FilterRedactionReader(DirectoryReader in, List<FieldRedactionMetadata> fieldRedactions)
            throws IOException {
        super(in, new RedactionSubReaderWrapper(fieldRedactions));
        this.fieldRedactions = fieldRedactions;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new FilterRedactionReader(in, fieldRedactions);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
