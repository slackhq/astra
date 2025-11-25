package com.slack.astra.blobfs;

import static com.slack.astra.blobfs.S3CachePagingLoader.DISK_PAGE_SIZE;
import static com.slack.astra.util.SizeConstant.GB;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.FileTransformerConfiguration;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/** Caches S3 data onto the disk, helping to reduce the penalty of S3 read latency. */
public class DiskCachePagingLoader {

  // Total size of the disk cache - may temporarily exceed this so some buffer should be available
  // on the host
  public static final String ASTRA_S3_STREAMING_DISK_CACHE_SIZE = "astra.s3Streaming.diskCacheSize";
  protected static final long DISK_CACHE_SIZE =
      Long.parseLong(
          System.getProperty(ASTRA_S3_STREAMING_DISK_CACHE_SIZE, String.valueOf(200 * GB)));

  private static final Logger LOG = LoggerFactory.getLogger(DiskCachePagingLoader.class);

  private final LoadingCache<LoadingCacheKey, Path> diskCache =
      Caffeine.newBuilder()
          .maximumWeight(DISK_CACHE_SIZE)
          .scheduler(Scheduler.systemScheduler())
          .evictionListener(evictionListener())
          .weigher(weigher())
          .build(this.bytesCacheLoader());

  private final BlobStore blobStore;
  private final S3AsyncClient s3AsyncClient;
  private final long pageSize;

  public DiskCachePagingLoader(BlobStore blobStore, S3AsyncClient s3AsyncClient, long pageSize) {
    this.blobStore = blobStore;
    this.s3AsyncClient = s3AsyncClient;
    this.pageSize = pageSize;
  }

  private static Weigher<LoadingCacheKey, Path> weigher() {
    return (_, value) -> {
      try {
        // todo - consider reworking weights to use kb instead of bytes? This will fail if files
        // exceed 2GB (int overflow)
        long fileSize = Files.size(value);
        LOG.debug("Calculated size of path {} is {} bytes", value, fileSize);
        return Math.toIntExact(fileSize);
      } catch (IOException e) {
        LOG.error("Error calculating size", e);
        // if we can't calculate just use the max page size
        return Math.toIntExact(DISK_PAGE_SIZE);
      }
    };
  }

  private static RemovalListener<LoadingCacheKey, Path> evictionListener() {
    return (cacheKey, path, removalCause) -> {
      if (cacheKey != null) {
        LOG.debug(
            "Evicting from disk cache - chunkID: {} / filename: {}, fromOffset: {}, toOffset: {} / cause: {}",
            cacheKey.getChunkId(),
            cacheKey.getFilename(),
            cacheKey.getFromOffset(),
            cacheKey.getToOffset(),
            removalCause);
      }
      if (path != null) {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          LOG.error("Failed to delete file {}, {}, {}", cacheKey, path, removalCause, e);
        }
      } else {
        LOG.error("Path was unexpectedly null, {}, {}, {}", cacheKey, path, removalCause);
      }
    };
  }

  private CacheLoader<LoadingCacheKey, Path> bytesCacheLoader() {
    return key -> {
      LOG.debug(
          "Using S3 to load disk cache - chunkID: {} / filename: {}, fromOffset: {}, toOffset: {}",
          key.getChunkId(),
          key.getFilename(),
          key.getFromOffset(),
          key.getToOffset());

      // todo - consider making this configurable to a different directory (or using the data dir
      // value)
      Path filePath =
          Path.of(
              System.getProperty("java.io.tmpdir"),
              String.format(
                  "astra-cache-%s-%s-%s-%s.tmp",
                  key.getChunkId(), key.getFilename(), key.getFromOffset(), key.getToOffset()));

      // First get the object metadata to determine actual file size
      long fileSize =
          s3AsyncClient
              .headObject(builder -> builder.bucket(blobStore.bucketName).key(key.getPath()))
              .get()
              .contentLength();

      // Cap the toOffset to not exceed the actual file size (0-indexed, so fileSize-1)
      long actualToOffset = Math.min(key.getToOffset(), fileSize - 1);

      s3AsyncClient
          .getObject(
              GetObjectRequest.builder()
                  .bucket(blobStore.bucketName)
                  .key(key.getPath())
                  .range(String.format("bytes=%s-%s", key.getFromOffset(), actualToOffset))
                  .build(),
              AsyncResponseTransformer.toFile(
                  filePath,
                  FileTransformerConfiguration.builder()
                      .failureBehavior(FileTransformerConfiguration.FailureBehavior.DELETE)
                      .fileWriteOption(
                          FileTransformerConfiguration.FileWriteOption.CREATE_OR_REPLACE_EXISTING)
                      .build()))
          .get();
      return filePath;
    };
  }

  public void readBytes(
      String chunkId,
      String filename,
      byte[] b,
      int startingOffset,
      long originalPointer,
      int totalLength)
      throws ExecutionException, IOException {
    // pointer here is the "global" file pointer
    int currentOffset = startingOffset;
    long currentPointer = originalPointer;
    int remainingLengthToRead = totalLength;

    for (LoadingCacheKey cacheKey : getCacheKeys(chunkId, filename, originalPointer, totalLength)) {
      // the relative pointer for the file on disk
      long relativePointer = currentPointer % pageSize;

      try (FileChannel fileChannel =
          FileChannel.open(diskCache.get(cacheKey), StandardOpenOption.READ)) {
        fileChannel.position(relativePointer);

        // if we need to read in everything
        if (currentPointer + remainingLengthToRead > cacheKey.getToOffset()) {
          // read from the relative pointer to the end
          int lengthToRead = Math.toIntExact(pageSize - relativePointer);
          ByteBuffer byteBuffer = ByteBuffer.wrap(b, currentOffset, lengthToRead);
          fileChannel.read(byteBuffer);

          currentOffset += lengthToRead;
          currentPointer += lengthToRead;
          remainingLengthToRead -= lengthToRead;
        } else {
          ByteBuffer byteBuffer = ByteBuffer.wrap(b, currentOffset, remainingLengthToRead);
          fileChannel.read(byteBuffer);
          break;
        }
      }
    }
  }

  public List<LoadingCacheKey> getCacheKeys(
      String chunkId, String filename, long originalPointer, int len) {
    long startingPage = Math.floorDiv(originalPointer, pageSize);
    long endingPage = Math.ceilDiv(originalPointer + len, pageSize);

    List<LoadingCacheKey> cacheKeys = new ArrayList<>(Math.toIntExact(endingPage - startingPage));
    for (long i = startingPage; i < endingPage; i++) {
      cacheKeys.add(
          new LoadingCacheKey(chunkId, filename, i * pageSize, i * pageSize + pageSize - 1));
    }
    return cacheKeys;
  }
}
