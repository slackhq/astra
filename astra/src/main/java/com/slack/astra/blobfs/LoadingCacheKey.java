package com.slack.astra.blobfs;

import java.util.Objects;

/** Cache key object for paging loaders, encapsulating required metadata for respective caches */
public class LoadingCacheKey {

  private final String path;
  private final String chunkId;
  private final String filename;
  private final Long fromOffset;
  private final Long toOffset;

  public LoadingCacheKey(String chunkId, String filename, long fromOffset, long toOffset) {
    this.chunkId = chunkId;
    this.filename = filename;
    this.path = String.format("%s/%s", chunkId, filename);
    this.fromOffset = fromOffset;
    this.toOffset = toOffset;
  }

  public LoadingCacheKey(String chunkId, String filename) {
    this.chunkId = chunkId;
    this.filename = filename;
    this.path = String.format("%s/%s", chunkId, filename);
    this.fromOffset = null;
    this.toOffset = null;
  }

  public String getChunkId() {
    return chunkId;
  }

  public String getFilename() {
    return filename;
  }

  public Long getFromOffset() {
    return fromOffset;
  }

  public Long getToOffset() {
    return toOffset;
  }

  public String getPath() {
    return path;
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LoadingCacheKey that)) return false;

    return path.equals(that.path)
        && chunkId.equals(that.chunkId)
        && filename.equals(that.filename)
        && Objects.equals(fromOffset, that.fromOffset)
        && Objects.equals(toOffset, that.toOffset);
  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + chunkId.hashCode();
    result = 31 * result + filename.hashCode();
    result = 31 * result + Objects.hashCode(fromOffset);
    result = 31 * result + Objects.hashCode(toOffset);
    return result;
  }

  @Override
  public String toString() {
    return "LoadingCacheKey{"
        + "chunkId='"
        + chunkId
        + '\''
        + ", path='"
        + path
        + '\''
        + ", filename='"
        + filename
        + '\''
        + ", fromOffset="
        + fromOffset
        + ", toOffset="
        + toOffset
        + '}';
  }
}
