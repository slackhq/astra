package com.slack.kaldb.metadata.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class SearchMetadataTest {
  @Test
  public void testSearchMetadata() {
    final String name = "testSearch";
    final String snapshotName = "testSnapshot";
    final String url = "http://10.10.1.1:9090";

    SearchMetadata searchMetadata = new SearchMetadata(name, snapshotName, url);

    assertThat(searchMetadata.name).isEqualTo(name);
    assertThat(searchMetadata.snapshotName).isEqualTo(snapshotName);
    assertThat(searchMetadata.url).isEqualTo(url);
  }

  @Test
  public void testEqualsAndHashCode() {
    final String name = "testSearch";
    final String snapshotName = "testSnapshot";
    final String url = "http://10.10.1.1:9090";

    SearchMetadata searchMetadata1 = new SearchMetadata(name, snapshotName, url);
    SearchMetadata searchMetadata2 = new SearchMetadata(name + "2", snapshotName, url);

    assertThat(searchMetadata1).isEqualTo(searchMetadata1);
    assertThat(searchMetadata1).isNotEqualTo(searchMetadata2);

    Set<SearchMetadata> set = new HashSet<>();
    set.add(searchMetadata1);
    set.add(searchMetadata2);
    assertThat(set.size()).isEqualTo(2);
    assertThat(set).containsOnly(searchMetadata1, searchMetadata2);
  }

  @Test
  public void testValidSearchMetadata() {
    final String name = "testSearch";
    final String snapshotName = "testSnapshot";
    final String url = "http://10.10.1.1:9090";
    final boolean isLive = true;

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SearchMetadata("", snapshotName, url));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SearchMetadata(null, snapshotName, url));
    assertThatIllegalArgumentException().isThrownBy(() -> new SearchMetadata(name, "", url));
    assertThatIllegalArgumentException().isThrownBy(() -> new SearchMetadata(name, null, url));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SearchMetadata(name, snapshotName, ""));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SearchMetadata(name, snapshotName, ""));
  }
}
