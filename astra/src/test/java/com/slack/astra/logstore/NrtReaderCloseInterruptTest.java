package com.slack.astra.logstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reproduces the indexer crash "FileLock invalidated by an external force" and shows why {@link
 * LuceneIndexStoreImpl} uses {@link SimpleFSLockFactory}.
 *
 * <p>A chunk-query thread holds an NRT searcher. While it searches, a merge removes the segments
 * that searcher references and the scheduled refresh swaps the SearcherManager to a new reader, so
 * the query thread is the last holder of the stale reader. ChunkManagerBase.query interrupts that
 * thread when the query times out. Releasing the searcher then closes the reader, which runs
 * IndexWriter file deletes through NativeFSLock.ensureValid -> FileChannel.size(); NIO closes the
 * channel because the thread is interrupted and the write.lock is invalid forever. The next flush
 * on any writer thread fails and the indexer halts.
 */
public class NrtReaderCloseInterruptTest {

  @TempDir Path tmp;

  private static IndexWriter newWriter(MMapDirectory dir) throws IOException {
    return new IndexWriter(
        dir,
        new IndexWriterConfig(new StandardAnalyzer())
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setUseCompoundFile(false));
  }

  private static void addDocs(IndexWriter w, int n, String prefix) throws IOException {
    for (int i = 0; i < n; i++) {
      Document d = new Document();
      d.add(new StringField("id", prefix + i, Field.Store.YES));
      d.add(new TextField("body", "hello " + prefix + " " + i, Field.Store.NO));
      w.addDocument(d);
    }
  }

  /**
   * Acquire a searcher, merge its segments away, refresh so our searcher is the last reference,
   * then release it with the interrupt flag optionally set. Returns the throwable from release.
   */
  private static Throwable releaseStaleSearcher(
      IndexWriter writer, SearcherManager mgr, boolean interruptBeforeRelease) throws Exception {
    addDocs(writer, 50, "a");
    mgr.maybeRefreshBlocking();
    IndexSearcher stale = mgr.acquire();

    addDocs(writer, 50, "b");
    writer.commit();
    writer.forceMerge(1);
    writer.commit();
    mgr.maybeRefreshBlocking(); // manager drops its ref; `stale` is now the only holder

    if (interruptBeforeRelease) {
      Thread.currentThread().interrupt();
    }
    try {
      mgr.release(stale);
      return null;
    } catch (Throwable t) {
      return t;
    } finally {
      Thread.interrupted();
    }
  }

  private void runScenario(LockFactory lockFactory, boolean interrupt, boolean expectWriterDead)
      throws Exception {
    try (MMapDirectory dir = new MMapDirectory(tmp.resolve("idx"), lockFactory);
        IndexWriter writer = newWriter(dir);
        SearcherManager mgr = new SearcherManager(writer, null)) {

      Throwable fromRelease = releaseStaleSearcher(writer, mgr, interrupt);

      if (expectWriterDead) {
        assertThat(fromRelease).isInstanceOf(ClosedByInterruptException.class);
        // Next write trips over the dead lock: "FileLock invalidated" becomes the tragedy, the
        // rollback throws ClosedChannelException releasing the already-closed lock channel, and
        // the indexer thread then sees "this IndexWriter is closed".
        assertThatThrownBy(
                () -> {
                  addDocs(writer, 1, "c");
                  mgr.maybeRefreshBlocking();
                })
            .isInstanceOfAny(ClosedChannelException.class, AlreadyClosedException.class);
        assertThat(writer.isOpen()).isFalse();
        assertThat(writer.getTragicException())
            .isInstanceOf(AlreadyClosedException.class)
            .hasMessageContaining("FileLock invalidated by an external force");
      } else {
        assertThat(fromRelease).isNull();
        addDocs(writer, 1, "c");
        assertThatCode(mgr::maybeRefreshBlocking).doesNotThrowAnyException();
        assertThatCode(writer::commit).doesNotThrowAnyException();
        assertThat(writer.isOpen()).isTrue();
      }
    }
  }

  @Test
  public void nativeLockDiesWhenInterruptedThreadReleasesLastStaleReader() throws Exception {
    runScenario(NativeFSLockFactory.INSTANCE, true, true);
  }

  @Test
  public void nativeLockSurvivesUninterruptedRelease() throws Exception {
    runScenario(NativeFSLockFactory.INSTANCE, false, false);
  }

  @Test
  public void simpleFSLockSurvivesInterruptedRelease() throws Exception {
    // SimpleFSLock.ensureValid uses Files.readAttributes, never a FileChannel.
    runScenario(SimpleFSLockFactory.INSTANCE, true, false);
  }

  @Test
  public void logStoreUsesSimpleFSLockFactory() throws Exception {
    try (LuceneIndexStoreImpl store =
        LuceneIndexStoreImpl.makeLogStore(
            tmp.resolve("store").toFile(),
            Duration.ofSeconds(10),
            Duration.ofSeconds(10),
            false,
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD,
            new SimpleMeterRegistry())) {
      assertThat(store.getDirectory().toString()).contains("SimpleFSLockFactory");
    }
  }
}
