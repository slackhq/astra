package com.slack.astra.metadata.search;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.metadata.core.AstraMetadata;
import com.slack.astra.proto.metadata.Metadata;
import java.util.List;

/** Search metadata contains the metadata needed to perform a search on a snapshot. */
public class SearchMetadata extends AstraMetadata {
  @Deprecated public final String snapshotName;

  public final String url;
  public final Metadata.SearchMetadata.SearchNodeType searchNodeType;
  public List<String> snapshotNames;

  public SearchMetadata(
      String name,
      String snapshotName,
      String url,
      Metadata.SearchMetadata.SearchNodeType searchNodeType,
      List<String> snapshotNames) {
    super(name);
    this.snapshotName = snapshotName;
    this.url = url;
    this.searchNodeType = searchNodeType;
    this.snapshotNames = snapshotNames;
  }

  public SearchMetadata(
      String name,
      String url,
      Metadata.SearchMetadata.SearchNodeType searchNodeType,
      List<String> snapshotNames) {
    super(name);
    this.url = url;
    this.searchNodeType = searchNodeType;
    this.snapshotNames = snapshotNames;
    this.snapshotName = "";
  }

  public SearchMetadata(String name, String snapshotName, String url) {
    super(name);
    checkArgument(url != null && !url.isEmpty(), "Url shouldn't be empty");
    this.snapshotName = snapshotName;
    this.url = url;
    this.snapshotNames = List.of(); // unmodifyable
    this.searchNodeType = Metadata.SearchMetadata.SearchNodeType.UNRECOGNIZED;
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

  public Metadata.SearchMetadata.SearchNodeType getSearchNodeType() {
    return searchNodeType;
  }

  public List<String> getSnapshotNames() {
    return snapshotNames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SearchMetadata that = (SearchMetadata) o;

    if (!snapshotName.equals(that.snapshotName)) return false;
    return url.equals(that.url);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + snapshotName.hashCode();
    result = 31 * result + url.hashCode();
    return result;
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
        + '}';
  }
}
