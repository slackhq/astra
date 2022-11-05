package com.slack.kaldb.logstore;

import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.document.Document;

public interface DocumentBuilder<T> {
  Document fromMessage(T message) throws IOException;

  Map<String, LuceneFieldDef> getSchema();
}
