package com.slack.kaldb.logstore;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.File;
import java.time.Duration;

/*
 * This class contains the config needed for LuceneIndexStore.
 */
public class LuceneIndexStoreConfig {

  public static final String DEFAULT_LOG_FILE_NAME = "indexer.log";
  public static final String DEFAULT_LOGS_FOLDER_NAME = "logs";
  public static final String DEFAULT_INDICES_FOLDER_NAME = "indices";

  // Controls how often the documents are flushed to disk.
  public final Duration commitDuration;

  // Controls how often the documents are visible for search.
  public final Duration refreshDuration;

  // The file system path for storing the files.
  public final String indexRoot;

  // The name of the lucene log file.
  public final String logFileName;

  // Config to set the IndexWriterConfig.setRAMBufferSizeMB.
  public final int ramBufferSizeMB;

  // A flag that turns on internal logging.
  public final boolean enableTracing;

  // TODO: Tweak the default values once in prod.
  static final Duration defaultCommitDuration = Duration.ofSeconds(15);
  static final Duration defaultRefreshDuration = Duration.ofSeconds(15);

  public static Duration getCommitDuration(KaldbConfigs.LuceneConfig luceneConfig) {
    final long commitDurationSecs = luceneConfig.getCommitDurationSecs();
    return commitDurationSecs != 0 ? Duration.ofSeconds(commitDurationSecs) : defaultCommitDuration;
  }

  public static Duration getRefreshDuration(KaldbConfigs.LuceneConfig luceneConfig) {
    final long refreshDurationSecs = luceneConfig.getRefreshDurationSecs();
    return refreshDurationSecs != 0
        ? Duration.ofSeconds(refreshDurationSecs)
        : defaultRefreshDuration;
  }

  public LuceneIndexStoreConfig(
      Duration commitDuration,
      Duration refreshDuration,
      String indexRoot,
      int ramBufferSizeMB,
      boolean enableTracing) {
    this(
        commitDuration,
        refreshDuration,
        indexRoot,
        DEFAULT_LOG_FILE_NAME,
        ramBufferSizeMB,
        enableTracing);
  }

  public LuceneIndexStoreConfig(
      Duration commitDuration,
      Duration refreshDuration,
      String indexRoot,
      String logFileName,
      int ramBufferSizeMB,
      boolean enableTracing) {
    ensureTrue(
        !(commitDuration.isZero() || commitDuration.isNegative()),
        "Commit duration should be greater than zero");
    ensureTrue(
        !(refreshDuration.isZero() || refreshDuration.isNegative()),
        "Commit duration should be greater than zero");
    this.commitDuration = commitDuration;
    this.refreshDuration = refreshDuration;
    this.indexRoot = indexRoot;
    this.logFileName = logFileName;
    this.ramBufferSizeMB = ramBufferSizeMB;
    this.enableTracing = enableTracing;
  }

  public File indexFolder(String id) {
    File indicesFolder = new File(indexRoot, DEFAULT_INDICES_FOLDER_NAME);
    return new File(indicesFolder, id);
  }

  public File logFolder(String id) {
    File logsFolder = new File(indexRoot, DEFAULT_LOGS_FOLDER_NAME);
    return new File(logsFolder, id);
  }
}
