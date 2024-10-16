package com.slack.astra.blobfs;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Lucene IndexInput, supporting partial reading of files from S3. Instead of
 * attempting to download an entire file to disk, this class will partially page in the bytes at the
 * time of read, caching the results in memory.
 */
public class S3IndexInput extends IndexInput {
  private static final Logger LOG = LoggerFactory.getLogger(S3IndexInput.class);

  private final String chunkId;
  private final String objectName;

  // pointer for next byte read within this input
  private long filePointer = 0;

  // variables if the input has been sliced
  private final long sliceOffset;
  private Long sliceLength = null;

  private final HeapCachePagingLoader heapCachePagingLoader;

  private S3IndexInput(
      String resourceDescription,
      HeapCachePagingLoader heapCachePagingLoader,
      String chunkId,
      String objectName,
      Long sliceOffset,
      Long length) {
    super(resourceDescription);
    this.chunkId = chunkId;
    this.objectName = objectName;
    this.filePointer = 0;
    this.sliceOffset = sliceOffset;
    this.sliceLength = length;
    this.heapCachePagingLoader = heapCachePagingLoader;
  }

  public S3IndexInput(
      BlobStore blobStore, String resourceDescription, String chunkId, String objectName) {
    super(resourceDescription);
    this.chunkId = chunkId;
    this.objectName = objectName;
    this.sliceOffset = 0;
    this.heapCachePagingLoader =
        S3CachePagingLoader.getInstance(blobStore, blobStore.s3AsyncClient);
  }

  @Override
  public void close() throws IOException {
    // nothing to close/cleanup
  }

  /** Returns the current position in this file, where the next read will occur. */
  @Override
  public long getFilePointer() {
    return filePointer;
  }

  /**
   * Sets current position in this file, where the next read will occur. If this is beyond the end
   * of the file then this will throw EOFException and then the stream is in an undetermined state.
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos > length()) {
      throw new EOFException();
    }
    filePointer = pos;
  }

  /** The number of bytes in the file. This value is cached to the fileLength variable. */
  @Override
  public long length() {
    if (sliceLength != null) {
      return sliceLength;
    }

    try {
      return heapCachePagingLoader.length(chunkId, objectName);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a slice of this index input, with the given description, offset, and length. The slice
   * is sought to the beginning.
   */
  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) {
    LOG.debug(
        "Slicing {} for chunk ID {}, offset {} length {}", objectName, chunkId, offset, length);
    return new S3IndexInput(
        sliceDescription, heapCachePagingLoader, chunkId, objectName, offset, length);
  }

  /** Reads and returns a single byte, paging in data if required. */
  @Override
  public byte readByte() {
    try {
      byte[] singeByte = new byte[1];
      readBytes(singeByte, 0, 1);
      return singeByte[0];
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads a specified number of bytes into an array at the specified offset.
   *
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len the number of bytes to read
   */
  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    try {
      heapCachePagingLoader.readBytes(
          chunkId, objectName, b, offset, filePointer + sliceOffset, len);
      filePointer += len;
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }
}
