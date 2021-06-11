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

  /*
   * This commit is protected from deletion
   * Users need to call releaseIndexCommitRef(IndexCommit) to free the underlying lucene resources once the work is complete
   */
  public IndexCommit getIndexCommit();

  public void releaseIndexCommitRef(IndexCommit indexCommit);

  // TODO: Add an isReadOnly and setReadOnly API here.

}
