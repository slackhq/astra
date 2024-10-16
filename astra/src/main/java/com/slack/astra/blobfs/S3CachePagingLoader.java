package com.slack.astra.blobfs;

import static com.slack.astra.util.SizeConstant.MB;

import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Singleton S3 cache paging loader. There should only ever be one instance of this class, even on
 * cache nodes.
 */
public class S3CachePagingLoader {

  // diskPageSize refers to the size of chunked file size on disk. This should be sufficiently large
  // to ensure good performance downloading from S3 (ie, latency), but small enough so unnecessary
  // data isn't downloaded.
  public static final String ASTRA_S3_STREAMING_DISK_PAGESIZE = "astra.s3Streaming.diskPageSize";
  protected static final long DISK_PAGE_SIZE =
      Long.parseLong(
          System.getProperty(ASTRA_S3_STREAMING_DISK_PAGESIZE, String.valueOf(100 * MB)));

  // heapPageSize is the amount of size cached per file read. This cache helps reduce the penalty of
  // reading data from disk.
  public static final String ASTRA_S3_STREAMING_HEAP_PAGESIZE = "astra.s3Streaming.heapPageSize";
  protected static final long HEAP_PAGE_SIZE =
      Long.parseLong(System.getProperty(ASTRA_S3_STREAMING_HEAP_PAGESIZE, String.valueOf(2 * MB)));

  private static HeapCachePagingLoader heapCachePagingLoader;

  private S3CachePagingLoader() {}

  public static HeapCachePagingLoader getInstance(
      BlobStore blobStore, S3AsyncClient s3AsyncClient) {
    if (heapCachePagingLoader == null) {
      DiskCachePagingLoader diskCachePagingLoader =
          new DiskCachePagingLoader(blobStore, s3AsyncClient, DISK_PAGE_SIZE);
      heapCachePagingLoader =
          new HeapCachePagingLoader(
              blobStore, s3AsyncClient, diskCachePagingLoader, HEAP_PAGE_SIZE);
    }
    return heapCachePagingLoader;
  }
}
