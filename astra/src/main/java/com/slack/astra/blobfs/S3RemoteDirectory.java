package com.slack.astra.blobfs;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal implementation of a Lucene Directory, supporting only the functions that are required to
 * support cache nodes reading data directly from S3.
 */
public class S3RemoteDirectory extends Directory {
  private static final Logger LOG = LoggerFactory.getLogger(S3RemoteDirectory.class);

  private final BlobStore blobStore;
  private final String chunkId;

  private String[] files = null;

  public S3RemoteDirectory(String chunkId, BlobStore blobStore) {
    this.chunkId = chunkId;
    this.blobStore = blobStore;
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
              .toArray(String[]::new);
      LOG.debug(
          "listed files for chunkId - {}, listResults - {}", chunkId, String.join(",", files));
    }
    return files;
  }

  @Override
  public void deleteFile(String name) {
    throw new NotImplementedException();
  }

  @Override
  public long fileLength(String name) {
    throw new NotImplementedException();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) {
    throw new NotImplementedException();
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
    throw new NotImplementedException();
  }

  @Override
  public void sync(Collection<String> names) {
    throw new NotImplementedException();
  }

  @Override
  public void syncMetaData() {
    throw new NotImplementedException();
  }

  @Override
  public void rename(String source, String dest) {
    throw new NotImplementedException();
  }

  @Override
  public IndexInput openInput(String name, IOContext context) {
    return new S3IndexInput(blobStore, name, chunkId, name);
  }

  @Override
  public Lock obtainLock(String name) {
    throw new NotImplementedException();
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing s3 remote directory");
  }

  @Override
  public Set<String> getPendingDeletions() {
    throw new NotImplementedException();
  }
}
