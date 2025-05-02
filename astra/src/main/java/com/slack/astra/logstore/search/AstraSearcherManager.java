package com.slack.astra.logstore.search;

import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3RemoteDirectory;
import com.slack.astra.logstore.search.fieldRedaction.RedactionFilterDirectoryReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A utility class that handles the full lifecycle of a SearcherManager and
// its dependencies
public class AstraSearcherManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AstraSearcherManager.class);
  private final SearcherManager searcherManager;
  private final Directory directory;

  public AstraSearcherManager(String chunkId, BlobStore blobStore) throws IOException {
    directory = new S3RemoteDirectory(chunkId, blobStore);
    DirectoryReader directoryReader = DirectoryReader.open(directory);

    RedactionFilterDirectoryReader reader = new RedactionFilterDirectoryReader(directoryReader);
    searcherManager = new SearcherManager(reader, null);
  }

  public AstraSearcherManager(Path path) throws IOException {
    directory = new MMapDirectory(path);
    DirectoryReader directoryReader = DirectoryReader.open(directory);

    RedactionFilterDirectoryReader reader = new RedactionFilterDirectoryReader(directoryReader);
    this.searcherManager = new SearcherManager(reader, null);
  }

  public AstraSearcherManager(OpenSearchDirectoryReader directoryReader) throws IOException {
    this.searcherManager = new SearcherManager(directoryReader, null);
    this.directory = null;
  }

  public SearcherManager getLuceneSearcherManager() {
    return this.searcherManager;
  }

  @Override
  public void close() throws IOException {
    if (this.directory != null) {
      try {
        this.directory.close();
      } catch (Exception e) {
        LOG.error("Failed to properly close directory: ", e);
      }
    }

    if (this.searcherManager != null) {
      try {
        this.searcherManager.close();
      } catch (Exception e) {
        LOG.error("Failed to properly close searcherManager: ", e);
      }
    }
  }
}
