package com.slack.astra.logstore;

import com.slack.astra.logstore.search.AstraSearcherManager;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.service.murron.trace.Trace;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.FSDirectory;

/* An interface that implements a read and write interface for the LogStore */
public interface LogStore extends Closeable {
  void addMessage(Trace.Span message);

  // TODO: Instead of exposing the searcherManager, consider returning an instance of the searcher.
  AstraSearcherManager getAstraSearcherManager();

  void commit();

  void refresh();

  /**
   * Final merge likely requires a following commit to ensure the data is appropriately flushed to
   * disk. Without a following commit this can result in work that is done as part of the final
   * merge that is never written to the disk.
   */
  void finalMerge();

  void cleanup() throws IOException;

  FSDirectory getDirectory();

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

  public void releaseIndexCommit(IndexCommit indexCommit);

  // Return the Schema used by the log store.
  public ConcurrentHashMap<String, LuceneFieldDef> getSchema();

  // TODO: Add an isReadOnly and setReadOnly API here.
}
