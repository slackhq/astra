package com.slack.kaldb.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.SearcherManager;

/* An interface that implements a read and write interface for the LogStore */
public interface LogStore<T> extends Closeable {
  void addMessage(T message);

  // TODO: Instead of exposing the searcherManager, consider returning an instance of the searcher.
  SearcherManager getSearcherManager();

  void commit();

  void refresh();

  boolean isOpen();

  void cleanup() throws IOException;

  Path getDirectory();

  /**
   * After a commit, lucene may merge multiple segments into one in the background. So, getting a
   * listing of files in a lucene index will not return a stable set of files. To address this, we
   * can get an IndexCommit, which provides a consistent set of files at a given point in time and
   * also protects the files from deletion. If a background merge happens after an indexCommit is
   * created, lucene holds on to those files on disk even if the background merge has marked that
   * file for deletion. So, we need to release the index commit object once it's no longer needed
   * using the releaseIndexCommit api call. Since lucene can always decide to run background merges
   * on an index, multiple calls to getIndexCommit, may return different lists of active files, even
   * if no new commits are performed on the index.
   */
  public IndexCommit getIndexCommit();

  // needed by JMH tests. Cannot add @VisibleForTesting on an abstract class
  public IndexWriter getIndexWriter();

  public void releaseIndexCommit(IndexCommit indexCommit);

  // TODO: Add an isReadOnly and setReadOnly API here.

}
