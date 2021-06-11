package com.slack.kaldb.logstore;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.SearcherManager;

/* An interface that implements a read and write interface for the LogStore */
public interface LogStore<T> {
  void addMessage(T message);

  // TODO: Instead of exposing the searcherManager, consider returning an instance of the searcher.
  SearcherManager getSearcherManager();

  void commit();

  void refresh();

  void close();

  boolean isOpen();

  void cleanup() throws IOException;

  Path getDirectory();

  public IndexCommit getIndexCommit();
  // TODO: Add an isReadOnly and setReadOnly API here.

  public void releaseIndexCommitRef(IndexCommit indexCommit);
}
