package com.slack.astra.blobfs;

import static com.slack.astra.util.SizeConstant.MB;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

/**
 * Implementation of the Lucene IndexInput, supporting partial reading of files from S3. Instead of
 * attempting to download an entire file to disk, this class will partially page in the bytes at the
 * time of read, caching the results in memory.
 */
public class S3IndexInput extends IndexInput {
  private static final Logger LOG = LoggerFactory.getLogger(S3IndexInput.class);
  protected static final String PAGE_COUNTER = "astra_s3_index_input_pagein_counter";

  public static final String ASTRA_S3_STREAMING_PAGESIZE = "astra.s3Streaming.pageSize";
  protected static final long PAGE_SIZE =
      Long.parseLong(System.getProperty(ASTRA_S3_STREAMING_PAGESIZE, String.valueOf(2 * MB)));

  private final BlobStore blobStore;
  private final S3AsyncClient s3AsyncClient;
  private final String chunkId;
  private final String objectName;
  private final Map<Long, byte[]> cachedData = new HashMap<>();

  // pointer for next byte read within this input
  private long filePointer = 0;
  private Long fileLength;

  // variables if the input has been sliced
  private final long sliceOffset;
  private Long sliceLength = null;

  private S3IndexInput(
      String resourceDescription,
      BlobStore blobStore,
      S3AsyncClient s3AsyncClient,
      String chunkId,
      String objectName,
      Long sliceOffset,
      Map<Long, byte[]> cachedData,
      Long length) {
    super(resourceDescription);
    this.blobStore = blobStore;
    this.s3AsyncClient = s3AsyncClient;
    this.chunkId = chunkId;
    this.objectName = objectName;

    this.filePointer = 0;
    this.sliceOffset = sliceOffset;

    this.cachedData.putAll(cachedData);
    this.sliceLength = length;
  }

  public S3IndexInput(
      BlobStore blobStore, String resourceDescription, String chunkId, String objectName) {
    super(resourceDescription);
    this.blobStore = blobStore;
    this.s3AsyncClient = blobStore.s3AsyncClient;
    this.chunkId = chunkId;
    this.objectName = objectName;
    this.sliceOffset = 0;
  }

  /**
   * Reads data from a cached variable, else pages in data for the given page offset. The offset is
   * a multiple of the page size, where pageKey 0 would be from bytes {0} - {pageSize}, pageKey 1
   * would be bytes {pageSize} to {2*pageSize}, etc.
   *
   * @param pageKey the offset to download
   */
  private byte[] getData(long pageKey) throws ExecutionException, InterruptedException {
    if (cachedData.containsKey(pageKey)) {
      return cachedData.get(pageKey);
    } else {
      Metrics.counter(PAGE_COUNTER, List.of(Tag.of("chunkId", chunkId))).increment();
      cachedData.clear();

      long readFrom = pageKey * PAGE_SIZE;
      long readTo = Math.min((pageKey + 1) * PAGE_SIZE, Long.MAX_VALUE);

      Stopwatch timeDownload = Stopwatch.createStarted();
      byte[] response =
          s3AsyncClient
              .getObject(
                  GetObjectRequest.builder()
                      .bucket(blobStore.bucketName)
                      .key(String.format("%s/%s", chunkId, objectName))
                      .range(String.format("bytes=%s-%s", readFrom, readTo))
                      .build(),
                  AsyncResponseTransformer.toBytes())
              .get()
              .asByteArray();

      LOG.debug(
          "Downloaded {} - byte length {} in {} ms for chunk {}",
          objectName,
          response.length,
          timeDownload.elapsed(TimeUnit.MILLISECONDS),
          chunkId);
      cachedData.put(pageKey, response);
      return response;
    }
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

    if (fileLength == null) {
      try {
        fileLength =
            s3AsyncClient
                .headObject(
                    HeadObjectRequest.builder()
                        .bucket(blobStore.bucketName)
                        .key(String.format("%s/%s", chunkId, objectName))
                        .build())
                .get()
                .contentLength();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error reading length", e);
        throw new RuntimeException(e);
      }
    }
    return fileLength;
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
        sliceDescription,
        blobStore,
        s3AsyncClient,
        chunkId,
        objectName,
        offset,
        cachedData,
        length);
  }

  /** Reads and returns a single byte, paging in data if required. */
  @Override
  public byte readByte() {
    try {
      long getCacheKey = Math.floorDiv(filePointer + sliceOffset, PAGE_SIZE);
      int byteArrayPos = Math.toIntExact(filePointer + sliceOffset - (getCacheKey * PAGE_SIZE));
      filePointer++;
      return getData(getCacheKey)[byteArrayPos];
    } catch (ExecutionException | InterruptedException e) {
      LOG.error("Error reading byte", e);
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
    for (int i = 0; i < len; i++) {
      b[offset + i] = readByte();
    }
  }

  @VisibleForTesting
  protected Map<Long, byte[]> getCachedData() {
    return new HashMap<>(cachedData);
  }
}
