package com.slack.astra.logstore.search;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.proto.metadata.Metadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AstraDistributedQueryServiceTestNew {

  @Test
  void testNodeWithNoCache() {

    List<SearchMetadata> nodes = new ArrayList<>();
    nodes.add(
        new SearchMetadata(
            "foo", "fooUrl", Metadata.SearchMetadata.SearchNodeType.CACHE, List.of("")));

    Map<String, List<String>> result =
        AstraDistributedQueryService.getQueryPlan(nodes, List.of("foo", "bar"));

    assertThat(result.get("fooUrl")).containsAll(List.of("foo", "bar"));
  }

  @Test
  void testTwoNodeWithNoCache() {

    List<SearchMetadata> nodes = new ArrayList<>();
    nodes.add(
        new SearchMetadata(
            "foo", "fooUrl", Metadata.SearchMetadata.SearchNodeType.CACHE, List.of("")));
    nodes.add(
        new SearchMetadata(
            "bar", "barUrl", Metadata.SearchMetadata.SearchNodeType.CACHE, List.of("")));

    Map<String, List<String>> result =
        AstraDistributedQueryService.getQueryPlan(nodes, List.of("foo", "bar"));

    assertThat(result.get("fooUrl").size()).isEqualTo(1);
    assertThat(result.get("barUrl").size()).isEqualTo(1);
  }

  @Test
  void testTwoNodeWithOneCache() {

    List<SearchMetadata> nodes = new ArrayList<>();
    nodes.add(
        new SearchMetadata(
            "foo", "fooUrl", Metadata.SearchMetadata.SearchNodeType.CACHE, List.of("")));
    nodes.add(
        new SearchMetadata(
            "bar", "barUrl", Metadata.SearchMetadata.SearchNodeType.CACHE, List.of("bar")));

    Map<String, List<String>> result =
        AstraDistributedQueryService.getQueryPlan(nodes, List.of("foo", "bar"));

    assertThat(result.get("fooUrl")).contains("foo");
    assertThat(result.get("barUrl")).contains("bar");
  }

  @Test
  void testTwoNodeWithOneCacheWithBoth() {

    List<SearchMetadata> nodes = new ArrayList<>();
    nodes.add(
        new SearchMetadata(
            "foo", "fooUrl", Metadata.SearchMetadata.SearchNodeType.CACHE, List.of("")));
    nodes.add(
        new SearchMetadata(
            "bar", "barUrl", Metadata.SearchMetadata.SearchNodeType.CACHE, List.of("foo", "bar")));

    Map<String, List<String>> result =
        AstraDistributedQueryService.getQueryPlan(nodes, List.of("foo", "bar"));

    assertThat(result.get("fooUrl").size()).isEqualTo(0);
    assertThat(result.get("barUrl")).containsAll(List.of("foo", "bar"));
  }
}
