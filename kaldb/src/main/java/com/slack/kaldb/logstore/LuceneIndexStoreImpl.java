package com.slack.kaldb.logstore;

import com.slack.kaldb.logstore.index.KalDBMergeScheduler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * LuceneIndexStore stores a log message in a lucene index. It uses LuceneIndexWriter to create an index. The config
 * defines the behavior of the index writer. The DocumentBuilder will decide how the document is analyzed before it is
 * stored in the index.
 *
 * TODO: Each index store has a unique id that is used to as a suffix/prefix in files associated with this store?
 */
public class LuceneIndexStoreImpl implements LogStore<LogMessage> {

  private final String id = UUID.randomUUID().toString();

  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexStoreImpl.class);
  public static final String MESSAGES_RECEIVED_COUNTER = "messages_received";
  public static final String MESSAGES_FAILED_COUNTER = "messages_failed";
  public static final String COMMITS_COUNTER = "commits";
  public static final String REFRESHES_COUNTER = "refreshes";

  private final SearcherManager searcherManager;
  private final LuceneIndexStoreConfig config;
  private final DocumentBuilder<LogMessage> documentBuilder;
  private final Analyzer analyzer;
  private final IndexWriterConfig indexWriterConfig;
  private final FSDirectory indexDirectory;
  private final Timer timer;
  private final SnapshotDeletionPolicy snapshotDeletionPolicy;
  private Optional<IndexWriter> indexWriter;

  // Stats counters.
  private final Counter messagesReceivedCounter;
  private final Counter messagesFailedCounter;
  private final Counter commitsCounter;
  private final Counter refreshesCounter;

  public static LuceneIndexStoreImpl makeLogStore(File dataDirectory, MeterRegistry metricsRegistry)
      throws IOException {
    return makeLogStore(
        dataDirectory,
        LuceneIndexStoreConfig.getCommitDuration(),
        LuceneIndexStoreConfig.getRefreshDuration(),
        metricsRegistry);
  }

  public static LuceneIndexStoreImpl makeLogStore(
      File dataDirectory,
      Duration commitInterval,
      Duration refreshInterval,
      MeterRegistry metricsRegistry)
      throws IOException {
    // TODO: Move all these config values into chunk?
    // TODO: Chunk should create log store?
    LuceneIndexStoreConfig indexStoreCfg =
        new LuceneIndexStoreConfig(
            commitInterval, refreshInterval, dataDirectory.getAbsolutePath(), 8, false);

    // TODO: set ignore property exceptions via CLI flag.
    return new LuceneIndexStoreImpl(
        indexStoreCfg, LogDocumentBuilderImpl.build(false), metricsRegistry);
  }

  public LuceneIndexStoreImpl(
      LuceneIndexStoreConfig config,
      DocumentBuilder<LogMessage> documentBuilder,
      MeterRegistry registry)
      throws IOException {

    this.config = config;
    this.documentBuilder = documentBuilder;

    this.analyzer = new StandardAnalyzer();
    this.snapshotDeletionPolicy =
        new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    indexWriterConfig =
        buildIndexWriterConfig(this.analyzer, this.snapshotDeletionPolicy, this.config, registry);
    indexDirectory = new NIOFSDirectory(config.indexFolder(id).toPath());
    indexWriter = Optional.of(new IndexWriter(indexDirectory, indexWriterConfig));
    this.searcherManager = new SearcherManager(indexWriter.get(), false, false, null);

    timer = new Timer(true);
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            commit();
          }
        },
        config.commitDuration.toMillis(),
        config.commitDuration.toMillis());
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            refresh();
          }
        },
        config.refreshDuration.toMillis(),
        config.refreshDuration.toMillis());

    // Initialize stats counters
    messagesReceivedCounter = registry.counter(MESSAGES_RECEIVED_COUNTER);
    messagesFailedCounter = registry.counter(MESSAGES_FAILED_COUNTER);
    commitsCounter = registry.counter(COMMITS_COUNTER);
    refreshesCounter = registry.counter(REFRESHES_COUNTER);

    LOG.info(
        "Created a lucene index {} at: {}", id, indexDirectory.getDirectory().toAbsolutePath());
  }

  private IndexWriterConfig buildIndexWriterConfig(
      Analyzer analyzer,
      SnapshotDeletionPolicy snapshotDeletionPolicy,
      LuceneIndexStoreConfig config,
      MeterRegistry metricsRegistry) {
    final IndexWriterConfig indexWriterCfg =
        new IndexWriterConfig(analyzer)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setRAMBufferSizeMB(config.ramBufferSizeMB)
            .setUseCompoundFile(false)
            .setMergeScheduler(new KalDBMergeScheduler(metricsRegistry))
            .setIndexDeletionPolicy(snapshotDeletionPolicy);

    if (config.enableTracing) {
      indexWriterCfg.setInfoStream(System.out);
    }

    return indexWriterCfg;
  }

  // TODO: IOException can be logged and recovered from?.
  private void syncCommit() throws IOException {
    synchronized (this) {
      if (indexWriter.isPresent()) {
        indexWriter.get().commit();
      }
    }
  }

  private void syncRefresh() throws IOException {
    synchronized (this) {
      if (indexWriter.isPresent()) {
        searcherManager.maybeRefresh();
      }
    }
  }

  @Override
  public Path getDirectory() {
    return indexDirectory.getDirectory();
  }

  private void handleNonFatal(Throwable ex) {
    messagesFailedCounter.increment();
    LOG.error(String.format("Exception %s processing", ex));
  }

  @Override
  public void addMessage(LogMessage message) {
    try {
      messagesReceivedCounter.increment();
      if (indexWriter.isPresent()) {
        indexWriter.get().addDocument(documentBuilder.fromMessage(message));
      } else {
        LOG.warn("IndexWriter should never be null when adding a message");
        throw new IllegalStateException("Index writer should never be null when adding a message");
      }
    } catch (PropertyTypeMismatchException
        | UnSupportedPropertyTypeException
        | IllegalArgumentException e) {
      LOG.error(String.format("Indexing message %s failed with error:", message), e);
      messagesFailedCounter.increment();
    } catch (IOException e) {
      // TODO: For now crash the program on IOException since it is likely a serious issue.
      // In future may need to handle this case more gracefully.
      e.printStackTrace();
    }
  }

  @Override
  public void commit() {
    LOG.debug("Indexer starting commit for: " + indexDirectory.getDirectory().toString());
    try {
      syncCommit();
      LOG.debug("Indexer finished commit for: " + indexDirectory.getDirectory().toString());
      commitsCounter.increment();
    } catch (IOException e) {
      handleNonFatal(e);
    }
  }

  @Override
  public void refresh() {
    LOG.debug("Indexer starting refresh for: " + indexDirectory.getDirectory().toString());
    try {
      syncRefresh();
      LOG.debug("Indexer finished refresh for: " + indexDirectory.getDirectory().toString());
      refreshesCounter.increment();
    } catch (IOException e) {
      handleNonFatal(e);
    }
  }

  @Override
  public boolean isOpen() {
    return indexWriter.isPresent();
  }

  @Override
  public String toString() {
    return "LuceneIndexStoreImpl{"
        + "id='"
        + id
        + '\''
        + ", at="
        + getDirectory().toAbsolutePath().toString()
        + '}';
  }

  @Override
  public IndexCommit getIndexCommit() {
    try {
      return snapshotDeletionPolicy.snapshot();
    } catch (IOException e) {
      LOG.error("Tried to snapshot index commit but failed", e);
    }
    return null;
  }

  @Override
  public IndexWriter getIndexWriter() {
    return indexWriter.get();
  }

  public void releaseIndexCommit(IndexCommit indexCommit) {
    if (indexCommit != null) {
      try {
        snapshotDeletionPolicy.release(indexCommit);
      } catch (IOException e) {
        LOG.warn("Tried to release snapshot index commit but failed", e);
      }
    }
  }

  /**
   * This method closes the log store cleanly and cancels any ongoing tasks. This function cancels
   * the existing timer but doesn't run a commit or refresh. The users of this class are need to
   * ensure that the data is already committed before close.
   */
  @Override
  public void close() {
    synchronized (this) {
      if (indexWriter.isEmpty()) {
        // Closable.close() requires this be idempotent, so silently exit instead of throwing an
        // exception
        return;
      }

      timer.cancel();
      try {
        indexWriter.get().close();
      } catch (IllegalStateException | IOException | NoSuchElementException e) {
        LOG.error("Error closing index " + id, e);
      }
      indexWriter = Optional.empty();
    }
  }

  // TODO: Currently, deleting the index. May need to delete the folder.
  @Override
  public void cleanup() throws IOException {
    if (indexWriter.isPresent()) {
      throw new IllegalStateException("IndexWriter should be closed before cleanup");
    }
    LOG.info("Deleting directory: {}", indexDirectory.getDirectory().toAbsolutePath());
    FileUtils.deleteDirectory(indexDirectory.getDirectory().toFile());
  }

  @Override
  public SearcherManager getSearcherManager() {
    return searcherManager;
  }
}
