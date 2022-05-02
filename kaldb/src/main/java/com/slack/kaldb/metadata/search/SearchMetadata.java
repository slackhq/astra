package com.slack.kaldb.metadata.search;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import java.util.Objects;

/** Search metadata contains the metadata needed to perform a search on a snapshot. */
public class SearchMetadata extends KaldbMetadata {
  public final String snapshotName;
  public final String url;
  public final boolean isLive;

  public SearchMetadata(String name, String snapshotName, String url, boolean isLive) {
    super(name);
    checkArgument(url != null && !url.isEmpty(), "Url shouldn't be empty");
    checkArgument(
        snapshotName != null && !snapshotName.isEmpty(), "SnapshotName should not be empty");
    this.snapshotName = snapshotName;
    this.url = url;
    this.isLive = isLive;
  }

  public static String generateSearchContextSnapshotId(String snapshotName, String hostname) {
    return snapshotName + "_" + hostname;
  }

  public String getSnapshotName() {
    return snapshotName;
  }

  public String getUrl() {
    return url;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SearchMetadata that = (SearchMetadata) o;
    return isLive == that.isLive && snapshotName.equals(that.snapshotName) && url.equals(that.url);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), snapshotName, url, isLive);
  }

  @Override
  public String toString() {
    return "SearchMetadata{"
        + "name='"
        + name
        + '\''
        + ", snapshotName='"
        + snapshotName
        + '\''
        + ", url='"
        + url
        + '\''
        + ", isLive='"
        + isLive
        + '\''
        + '}';
  }
}
