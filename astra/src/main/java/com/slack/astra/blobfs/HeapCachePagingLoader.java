package com.slack.astra.blobfs;

import static com.slack.astra.util.SizeConstant.GB;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

/** Caches S3 data into the heap, passing through to the disk cache if the data is not available. */
public class HeapCachePagingLoader {
  private static final Logger LOG = LoggerFactory.getLogger(HeapCachePagingLoader.class);

  public static final String ASTRA_S3_STREAMING_HEAP_CACHE_SIZE = "astra.s3Streaming.heapCacheSize";
  protected static final long HEAP_CACHE_SIZE =
      Long.parseLong(
          System.getProperty(ASTRA_S3_STREAMING_HEAP_CACHE_SIZE, String.valueOf(1 * GB)));

  private final LoadingCache<LoadingCacheKey, byte[]> heapCache =
      Caffeine.newBuilder()
          .maximumWeight(HEAP_CACHE_SIZE)
          .scheduler(Scheduler.systemScheduler())
          .removalListener(heapRemovalListener())
          .weigher(weigher())
          .build(this.bytesCacheLoader());

  private final LoadingCache<LoadingCacheKey, Long> fileLengthCache =
      Caffeine.newBuilder().maximumSize(25000).build(this.fileLengthLoader());

  private final BlobStore blobStore;
  private final S3AsyncClient s3AsyncClient;
  private final DiskCachePagingLoader diskCachePagingLoader;
  private final long pageSize;

  public HeapCachePagingLoader(
      BlobStore blobStore,
      S3AsyncClient s3AsyncClient,
      DiskCachePagingLoader diskCachePagingLoader,
      long pageSize) {
    this.blobStore = blobStore;
    this.s3AsyncClient = s3AsyncClient;
    this.diskCachePagingLoader = diskCachePagingLoader;
    this.pageSize = pageSize;
  }

  private static Weigher<LoadingCacheKey, byte[]> weigher() {
    return (_, value) -> value.length;
  }

  private static RemovalListener<LoadingCacheKey, byte[]> heapRemovalListener() {
    return (key, _, cause) -> {
      if (key != null) {
        LOG.debug(
            "Evicting from heap cache - chunkID: {} / filename: {}, fromOffset: {}, toOffset: {} / cause: {}",
            key.getChunkId(),
            key.getFilename(),
            key.getFromOffset(),
            key.getToOffset(),
            cause);
      }
    };
  }

  private CacheLoader<LoadingCacheKey, byte[]> bytesCacheLoader() {
    return key -> {
      LOG.debug(
          "Using disk cache to load heap cache - chunkID: {} / filename: {}, fromOffset: {}, toOffset: {}",
          key.getChunkId(),
          key.getFilename(),
          key.getFromOffset(),
          key.getToOffset());
      int length = Math.toIntExact(key.getToOffset() - key.getFromOffset() + 1);
      byte[] bytes = new byte[length];
      diskCachePagingLoader.readBytes(
          key.getChunkId(), key.getFilename(), bytes, 0, key.getFromOffset(), length);
      return bytes;
    };
  }

  private CacheLoader<LoadingCacheKey, Long> fileLengthLoader() {
    return key ->
        s3AsyncClient
            .headObject(
                HeadObjectRequest.builder().bucket(blobStore.bucketName).key(key.getPath()).build())
            .get()
            .contentLength();
  }

  public void readBytes(
      String chunkId,
      String filename,
      byte[] b,
      int startingOffset,
      long originalPointer,
      int totalLength)
      throws ExecutionException, IOException {
    int currentOffset = startingOffset;
    long currentPointer = originalPointer;
    int remainingLengthToRead = totalLength;

    for (LoadingCacheKey cacheKey : getCacheKeys(chunkId, filename, originalPointer, totalLength)) {
      long relativePointer = currentPointer % pageSize;
      // if we need to read in everything
      if (currentPointer + remainingLengthToRead > cacheKey.getToOffset()) {
        // read from the relative pointer to the end
        int lengthToRead = Math.toIntExact(pageSize - relativePointer);
        System.arraycopy(
            heapCache.get(cacheKey),
            Math.toIntExact(relativePointer),
            b,
            currentOffset,
            lengthToRead);

        currentOffset += lengthToRead;
        currentPointer += lengthToRead;
        remainingLengthToRead -= lengthToRead;
      } else {
        System.arraycopy(
            heapCache.get(cacheKey),
            Math.toIntExact(relativePointer),
            b,
            currentOffset,
            remainingLengthToRead);
        break;
      }
    }
  }

  public long length(String chunkId, String filename) throws ExecutionException {
    return fileLengthCache.get(new LoadingCacheKey(chunkId, filename));
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
