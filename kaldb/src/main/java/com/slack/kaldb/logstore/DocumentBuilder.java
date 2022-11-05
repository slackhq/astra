package com.slack.kaldb.logstore;

import com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.document.Document;

public interface DocumentBuilder<T> {
  Document fromMessage(T message) throws IOException;

  // TODO: Move field def into it's own class.
  Map<String, SchemaAwareLogDocumentBuilderImpl.FieldDef> getSchema();
}
