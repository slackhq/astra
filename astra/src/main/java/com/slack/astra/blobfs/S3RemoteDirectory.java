package com.slack.astra.blobfs;

import com.slack.astra.chunk.ReadOnlyChunkImpl;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RemoteDirectory extends Directory {
  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyChunkImpl.class); //

  private final BlobStore blobStore;
  private final String chunkId;

  private List<String> files = null;

  // todo - a disk-based cache here would be a cache for the chunkId
  // todo - preload

  public S3RemoteDirectory(String chunkId, BlobStore blobStore) {
    this.chunkId = chunkId;
    this.blobStore = blobStore;

    // todo - throw nosuchfile if does not exist
  }

  @Override
  public String[] listAll() {
    if (files == null) {
      files =
          blobStore.listFiles(chunkId).stream()
              .map(
                  fullPath -> {
                    String[] parts = fullPath.split("/");
                    return parts[parts.length - 1];
                  })
              .filter(
                  (predicate) -> {
                    // todo - this shouldn't be necessary
                    return !predicate.contains("write.lock");
                  })
              .toList();
      LOG.debug(
          "listed files for chunkId - {}, listResults - {}", chunkId, String.join(",", files));
    }
    return files.toArray(String[]::new);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    LOG.error("Method not implemented");
    throw new NotImplementedException();
  }

  @Override
  public long fileLength(String name) throws IOException {
    LOG.error("Method not implemented");
    throw new NotImplementedException();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    LOG.error("Method not implemented");
    throw new NotImplementedException();
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    LOG.error("Method not implemented");
    throw new NotImplementedException();
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    LOG.error("Method not implemented");
    throw new NotImplementedException();
  }

  @Override
  public void syncMetaData() throws IOException {
    LOG.error("Method not implemented");
    throw new NotImplementedException();
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    LOG.error("Method not implemented");
    throw new NotImplementedException();
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return new S3IndexInput(blobStore, name, chunkId, name);
  }

  @Override
  public Lock obtainLock(String name) throws IOException {
    LOG.error("Method not implemented");
    throw new NotImplementedException();
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing s3 remote directory");
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    LOG.error("Method not implemented");
    throw new NotImplementedException();
  }
}
