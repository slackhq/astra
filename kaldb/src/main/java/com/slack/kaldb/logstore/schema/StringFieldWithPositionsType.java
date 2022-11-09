package com.slack.kaldb.logstore.schema;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

public class StringFieldWithPositionsType extends Field {

    /** Indexed, not tokenized, omits norms, indexes DOCS_AND_FREQS_AND_POSITIONS, not stored. */
    public static final FieldType TYPE_NOT_STORED = new FieldType();

    /** Indexed, not tokenized, omits norms, indexes DOCS_AND_FREQS_AND_POSITIONS, stored */
    public static final FieldType TYPE_STORED = new FieldType();

    static {
        TYPE_NOT_STORED.setOmitNorms(true);
        TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        TYPE_NOT_STORED.setTokenized(false);
        TYPE_NOT_STORED.freeze();

        TYPE_STORED.setOmitNorms(true);
        TYPE_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        TYPE_STORED.setStored(true);
        TYPE_STORED.setTokenized(false);
        TYPE_STORED.freeze();
    }

    public StringFieldWithPositionsType(String name, String value, Store stored) {
        super(name, value, stored == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
    }
}
