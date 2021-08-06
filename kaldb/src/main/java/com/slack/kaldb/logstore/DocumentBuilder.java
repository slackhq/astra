package com.slack.kaldb.logstore;

import java.io.IOException;
import org.apache.lucene.document.Document;

public interface DocumentBuilder<T> {
  Document fromMessage(T message) throws IOException;
}
