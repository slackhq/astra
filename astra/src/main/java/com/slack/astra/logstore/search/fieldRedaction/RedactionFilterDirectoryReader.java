package com.slack.astra.logstore.search.fieldRedaction;

import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.opensearch.common.collect.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// filter reader > sub reader wrapper > leaf reader > stored field reader,
//   which uses the stored field visitor to perform the field level redactions
// Heavily inspired by the DlsFlsFilterLeafReader from opensearch
// https://github.com/opensearch-project/security/blob/4f2e689a37765786dd256a7591434815bbb950a4/src/main/java/org/opensearch/security/configuration/DlsFlsFilterLeafReader.java

// Implements a filterdirectoryreader for field redaction
public class RedactionFilterDirectoryReader extends FilterDirectoryReader {
    private final Map<String, FieldRedactionMetadata> fieldRedactionsMap;
    private final FieldRedactionMetadataStore fieldRedactionsStore;

    public RedactionFilterDirectoryReader(DirectoryReader in, FieldRedactionMetadataStore fieldRedactionsStore)
            throws IOException {
        super(in, new RedactionSubReaderWrapper(fieldRedactionsMap));

        Map<String, FieldRedactionMetadata> fieldRedactionsMap = new HashMap<>();
        // todo - listener on metadatastore here
        AstraMetadataStoreChangeListener listener = new AstraMetadataStoreChangeListener() {
            @Override
            public void onMetadataStoreChanged(Object model) {
                fieldRedactionsStore.listSync().forEach(redaction -> {
                    fieldRedactionsMap.put(redaction.getFieldName(), redaction);
                });

            }
        };

        fieldRedactionsStore.addListener(listener);

        this.fieldRedactionsMap = fieldRedactionsMap;
        this.fieldRedactionsStore = fieldRedactionsStore;

    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new RedactionFilterDirectoryReader(in, fieldRedactionsMap);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    @Override
    protected void doClose() throws IOException {
        super.doClose();
        // todo - not sure if this close is correct
        this.fieldRedactionsStore.close();
    }
}
