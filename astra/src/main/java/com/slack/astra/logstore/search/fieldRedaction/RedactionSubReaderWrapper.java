package com.slack.astra.logstore.search.fieldRedaction;

import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;

/**
 * RedactionSubReaderWrapper is called by the RedactionFilterDirectoryReader as a wrapper to the
 * reader, and creates a RedactionLeafReader.
 */
class RedactionSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {

  public RedactionSubReaderWrapper() {}

  @Override
  public LeafReader wrap(LeafReader reader) {
    return new RedactionLeafReader(reader);
  }
}
