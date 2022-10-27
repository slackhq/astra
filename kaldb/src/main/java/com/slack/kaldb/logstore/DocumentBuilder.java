package com.slack.kaldb.logstore;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.document.Document;

public interface DocumentBuilder<T> {
  Document fromMessage(T message) throws IOException;

  Map<String, SchemaAwareLogDocumentBuilderImpl.FieldDef> getSchema();
}
