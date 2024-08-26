package com.slack.astra.blobfs;

import static com.slack.astra.util.SizeConstant.MB;

import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

public class S3IndexInput extends IndexInput {
  private static final Logger LOG = LoggerFactory.getLogger(S3IndexInput.class);
  private final BlobStore blobStore;
  private final S3AsyncClient s3AsyncClient;

  private final String chunkId;
  private final String objectName;

  private final long pageSize = 1 * MB;

  private final Map<Long, byte[]> cachedData = new HashMap<>();

  private long relativePos = 0;
  private Long size;
  private Long fileOffset = 0L;
  private Long maxLength = null;

  public S3IndexInput(
      String resourceDescription,
      BlobStore blobStore,
      S3AsyncClient s3AsyncClient,
      String chunkId,
      String objectName,
      Long fileOffset,
      Map<Long, byte[]> cachedData,
      Long length) {

    super(resourceDescription);
    this.blobStore = blobStore;
    this.s3AsyncClient = s3AsyncClient;
    this.chunkId = chunkId;
    this.objectName = objectName;

    this.relativePos = 0;
    this.fileOffset = fileOffset;

    this.cachedData.putAll(cachedData);
    this.maxLength = length;
  }

  public S3IndexInput(
      BlobStore blobStore, String resourceDescription, String chunkId, String objectName) {
    super(resourceDescription);
    this.blobStore = blobStore;
    this.s3AsyncClient = blobStore.getS3AsyncClient();
    this.chunkId = chunkId;
    this.objectName = objectName;
  }

  private byte[] getData(long key) throws ExecutionException, InterruptedException {
    if (cachedData.containsKey(key)) {
      return cachedData.get(key);
    } else {
      cachedData.clear();

      long readFrom = key * pageSize;

      // todo - does using size help at all?
      long readTo = Math.min((key + 1) * pageSize, Long.MAX_VALUE);

      LOG.debug(
          "Attempting to download {}, currentPos {}, key {}, bucketName {}, from {} to {} for chunk {}",
          objectName,
          relativePos,
          key,
          blobStore.getBucketName(),
          readFrom,
          readTo,
          chunkId);

      Stopwatch timeDownload = Stopwatch.createStarted();
      byte[] response =
          s3AsyncClient
              .getObject(
                  GetObjectRequest.builder()
                      .bucket(blobStore.getBucketName())
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
      cachedData.put(key, response);
      return response;
    }
  }

  @Override
  public void close() throws IOException {
    // nothing to close/cleanup
  }

  @Override
  public long getFilePointer() {
    return relativePos;
  }

  @Override
  public void seek(long pos) {
    relativePos = pos;
  }

  @Override
  public long length() {
    if (maxLength != null) {
      return maxLength;
    }

    if (size == null) {
      try {
        size =
            s3AsyncClient
                .headObject(
                    HeadObjectRequest.builder()
                        .bucket(blobStore.getBucketName())
                        .key(String.format("%s/%s", chunkId, objectName))
                        .build())
                .get()
                .contentLength();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error reading length", e);
        throw new RuntimeException(e);
      }
    }
    return size;
  }

  /**
   * Creates a slice of this index input, with the given description, offset, and length. The slice
   * is sought to the beginning.
   */
  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
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

  @Override
  public byte readByte() {
    try {
      long getCacheKey = Math.floorDiv(relativePos + fileOffset, pageSize);
      int byteArrayPos = Math.toIntExact(relativePos + fileOffset - (getCacheKey * pageSize));
      relativePos++;
      return getData(getCacheKey)[byteArrayPos];
    } catch (ExecutionException | InterruptedException e) {
      LOG.error("Error reading byte", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    long getCacheKey = Math.floorDiv(relativePos + fileOffset, pageSize);
    if (relativePos + fileOffset + len > ((getCacheKey + 1) * pageSize)) {
      LOG.debug(
          "Will need to page in content for {}, currentPos {}, len {}, currentCacheKey {}, chunkId {}",
          objectName,
          relativePos + fileOffset,
          len,
          getCacheKey,
          chunkId);
    }

    for (int i = 0; i < len; i++) {
      b[offset + i] = readByte();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>Warning:</b> Lucene never closes cloned {@code IndexInput}s, it will only call {@link
   * #close()} on the original object.
   *
   * <p>If you access the cloned IndexInput after closing the original object, any <code>readXXX
   * </code> methods will throw {@link AlreadyClosedException}.
   *
   * <p>This method is NOT thread safe, so if the current {@code IndexInput} is being used by one
   * thread while {@code clone} is called by another, disaster could strike.
   */
  @Override
  public IndexInput clone() {
    // todo - instead of an entirely new object consider reworking this?
    LOG.debug("Cloning object - chunkId {}, objectName {}", chunkId, objectName);
    return super.clone();
  }
}
