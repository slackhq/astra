package com.slack.kaldb.logstore;

import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.document.Document;

/**
 * DocumentBuilder defines the interfaces for classes that generate Lucene documents out of
 * messages.
 */
public interface DocumentBuilder<T> {
  Document fromMessage(T message) throws IOException;

  ConcurrentHashMap<String, LuceneFieldDef> getSchema();
}
