package com.slack.kaldb.logstore.search;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;

public class UniqueFieldTrackingSearcherFactory extends SearcherFactory {

  public Set<FieldInfos> allFields;

  public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader)
      throws IOException {
    for (LeafReaderContext segmentReaderContext : reader.getContext().leaves()) {
      FieldInfos fieldInfos = segmentReaderContext.reader().getFieldInfos();
      // merge all field infos from all the segments and cache it in allFields
    }
    return new IndexSearcher(reader);
  }
}
