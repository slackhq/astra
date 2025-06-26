package com.slack.astra.metadata.search;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.metadata.core.AstraMetadata;

/** Search metadata contains the metadata needed to perform a search on a snapshot. */
public class SearchMetadata extends AstraMetadata {
  public final String snapshotName;
  public final String url;
  private Boolean searchable;

  public SearchMetadata(String name, String snapshotName, String url) {
    this(name, snapshotName, url, true);
  }

  public SearchMetadata(String name, String snapshotName, String url, Boolean searchable) {
    super(name);
    checkArgument(searchable != null, "searchable cannot be null");
    checkArgument(url != null && !url.isEmpty(), "Url shouldn't be empty");
    checkArgument(
        snapshotName != null && !snapshotName.isEmpty(), "SnapshotName should not be empty");
    this.snapshotName = snapshotName;
    this.url = url;
    this.searchable = searchable;
  }

  public static String generateSearchContextSnapshotId(String snapshotName, String hostname) {
    return snapshotName + "_" + hostname;
  }

  public Boolean isSearchable() {
    if (searchable == null) {
      return true;
    }
    return searchable;
  }

  public void setSearchable(Boolean searchable) {
    this.searchable = searchable;
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
    if (!(o instanceof SearchMetadata)) return false;
    if (!super.equals(o)) return false;

    SearchMetadata that = (SearchMetadata) o;

    if (!snapshotName.equals(that.snapshotName)) return false;
    if (!searchable.equals(that.searchable)) return false;

    return url.equals(that.url);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + snapshotName.hashCode();
    result = 31 * result + url.hashCode();
    result = 31 * result + searchable.hashCode();
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
        + ", searchable="
        + searchable
        + '}';
  }
}
