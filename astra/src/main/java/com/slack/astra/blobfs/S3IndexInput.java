package com.slack.astra.blobfs;

import static com.slack.astra.util.SizeConstant.MB;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import io.micrometer.core.instrument.Metrics;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.FileTransformerConfiguration;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
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
      Long.parseLong(System.getProperty(ASTRA_S3_STREAMING_PAGESIZE, String.valueOf(100 * MB)));

  private final BlobStore blobStore;
  private final S3AsyncClient s3AsyncClient;
  private final String chunkId;
  private final String objectName;

  private long cacheKey = -1;
  private final Path tmpFile;

  private BufferedRandomAccessFile randomAccessFile;// = new RandomAccessFile();
  //private FileChannel fileChannel;
  //private FileInputStream fileInputStream;

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
      long cacheKey,
      Path inFile,
      Long length) {
    super(resourceDescription);
    this.blobStore = blobStore;
    this.s3AsyncClient = s3AsyncClient;
    this.chunkId = chunkId;
    this.objectName = objectName;

    this.filePointer = 0;
    this.sliceOffset = sliceOffset;
    this.sliceLength = length;

   this.cacheKey = cacheKey;

    try {
      Path slicePath = Files.createTempFile(String.format("astra-cache-slice-%s-%s-%s", chunkId, UUID.randomUUID(), resourceDescription), ".tmp");
      this.tmpFile = Files.copy(inFile, slicePath);

      //fileChannel
      this.randomAccessFile = new BufferedRandomAccessFile(tmpFile.toFile(), "r");
      //this.fileInputStream = new FileInputStream(tmpFile.toFile());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public S3IndexInput(
      BlobStore blobStore, String resourceDescription, String chunkId, String objectName) {
    super(resourceDescription);

    try {
      this.tmpFile = Files.createTempFile(String.format("astra-cache-%s-%s", chunkId, resourceDescription), ".tmp");
      this.randomAccessFile = new BufferedRandomAccessFile(tmpFile.toFile(), "rw");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

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
  private boolean getData(long pageKey) throws ExecutionException, InterruptedException, IOException {
    if (this.cacheKey == pageKey) {
      // return byte array from file
      return false;
    } else {
      Metrics.counter(PAGE_COUNTER).increment();

      long readFrom = pageKey * PAGE_SIZE;
      long readTo = Math.min((pageKey + 1) * PAGE_SIZE, Long.MAX_VALUE);

      Stopwatch timeDownload = Stopwatch.createStarted();
      GetObjectResponse response =
      s3AsyncClient
          .getObject(
              GetObjectRequest.builder()
                  .bucket(blobStore.bucketName)
                  .key(String.format("%s/%s", chunkId, objectName))
                  .range(String.format("bytes=%s-%s", readFrom, readTo))
                  .build(),
              AsyncResponseTransformer.toFile(tmpFile, FileTransformerConfiguration.builder()
                      .failureBehavior(FileTransformerConfiguration.FailureBehavior.DELETE)
                      .fileWriteOption(FileTransformerConfiguration.FileWriteOption.CREATE_OR_REPLACE_EXISTING)
                  .build()))
          .get();

      LOG.info(
          "Downloaded {} - byte length {} in {} ms for chunk {}",
          objectName,
          response.contentLength(),
          timeDownload.elapsed(TimeUnit.MILLISECONDS),
          chunkId);

      cacheKey = pageKey;
      return true;
    }
  }

  @Override
  public void close() throws IOException {
    randomAccessFile.close();
    tmpFile.toFile().delete();
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
        cacheKey,
        tmpFile,
        length);
  }

  /** Reads and returns a single byte, paging in data if required. */
  @Override
  public byte readByte() {
    try {
      long getCacheKey = Math.floorDiv(filePointer + sliceOffset, PAGE_SIZE);
      int byteArrayPos = Math.toIntExact(filePointer + sliceOffset - (getCacheKey * PAGE_SIZE));
      filePointer++;

      // page it in, if needed from S3
      boolean shouldReloadBuffer = getData(getCacheKey);
      if (shouldReloadBuffer) {
        randomAccessFile.reset();
      }

      //if (pagedIn) {
        randomAccessFile.seek(byteArrayPos);
      //}


        return randomAccessFile.readByte();
       // return fileInputStream.readNBytes(1)[0];
      //}

      //try (Files.newByteChannel()
      //return getData(getCacheKey)[byteArrayPos];
    } catch (ExecutionException | InterruptedException | IOException e) {
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

  protected long getCacheKey() {
    return this.cacheKey;
  }

  protected byte[] getCacheData() throws IOException {
    return Files.readAllBytes(tmpFile);
  }
}
