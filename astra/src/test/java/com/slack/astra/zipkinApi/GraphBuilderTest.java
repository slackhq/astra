package com.slack.astra.zipkinApi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GraphBuilderTest {
  private GraphBuilder graphBuilder;

  @BeforeEach
  void setUp() {
    graphBuilder = new GraphBuilder();
  }

  @Test
  void buildFromSpans_emptyList_returnsEmptyGraph() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).isEmpty();
    assertThat(graph.edges).isEmpty();
  }

  @Test
  void buildFromSpans_singleSpanWithoutParent_createsSingleNodeWithoutEdges() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    ZipkinSpanResponse span =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app",
                "app1",
                "kube.namespace",
                "ns1",
                "kube.operation",
                "op1",
                "resource",
                "res1"));
    spans.add(span);

    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);

    assertThat(node.getId()).isEqualTo("app1:ns1:res1");
    assertThat(node.getApp()).isEqualTo("app1");
    assertThat(node.getNamespace()).isEqualTo("ns1");
    assertThat(node.getOperation()).isEqualTo("op1");
    assertThat(node.getResource()).isEqualTo("res1");

    assertThat(graph.edges).isEmpty();
  }

  @Test
  void buildFromSpans_parentChildSpans_createsNodesWithEdge() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    ZipkinSpanResponse parentSpan =
        createSpanWithTags(
            "parent1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    ZipkinSpanResponse childSpan =
        createSpanWithTags(
            "child1",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app2",
                "kube.namespace", "ns2",
                "kube.operation", "op2",
                "resource", "res2"));

    spans.add(parentSpan);
    spans.add(childSpan);

    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(2);
    assertThat(graph.edges).hasSize(1);

    DependencyLink edge = graph.edges.iterator().next();
    assertThat(edge.parent()).isEqualTo("app1:ns1:res1");
    assertThat(edge.child()).isEqualTo("app2:ns2:res2");
  }

  @Test
  void buildFromSpans_httpRequestSpan_usesCanonicalPathAsResource() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    ZipkinSpanResponse span =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "http.request",
                "resource", "original_resource",
                "tag.operation.canonical_path", "/api/users"));
    spans.add(span);

    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);

    assertThat(node.getResource()).isEqualTo("/api/users");
    assertThat(node.getId()).isEqualTo("app1:ns1:/api/users");
  }

  @Test
  void buildFromSpans_httpRequestSpanWithoutCanonicalPath_usesOriginalResource() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse span =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "http.request",
                "resource", "original_resource"));
    spans.add(span);

    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);
    assertThat(node.getResource()).isEqualTo("original_resource");
  }

  @Test
  void buildFromSpans_missingTags_usesDefaultValues() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse span = createSpanWithTags("span1", "trace1", null, Map.of());
    spans.add(span);

    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);

    assertThat(node.getApp()).isEqualTo("unknown_app");
    assertThat(node.getNamespace()).isEqualTo("unknown_namespace");
    assertThat(node.getOperation()).isEqualTo("unknown_operation");
    assertThat(node.getResource()).isEqualTo("unknown_resource");
    assertThat(node.getId()).isEqualTo("unknown_app:unknown_namespace:unknown_resource");
  }

  @Test
  void buildFromSpans_spanWithNullId_skipsSpan() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse validSpan =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    ZipkinSpanResponse invalidSpan = new ZipkinSpanResponse(null, "trace1");
    invalidSpan.setTags(
        Map.of(
            "kube.app", "app2",
            "kube.namespace", "ns2",
            "kube.operation", "op2",
            "resource", "res2"));

    spans.add(validSpan);
    spans.add(invalidSpan);

    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);
    assertThat(node.getId()).isEqualTo("app1:ns1:res1");
  }

  @Test
  void buildFromSpans_childSpanWithNonExistentParent_createsChildNodeWithoutEdge() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse childSpan =
        createSpanWithTags(
            "child1",
            "trace1",
            "nonexistent_parent",
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    spans.add(childSpan);

    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    assertThat(graph.edges).isEmpty();
  }

  @Test
  void buildFromSpans_duplicateNodes_deduplicatesNodes() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    // two spans that would create the same node
    ZipkinSpanResponse span1 =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    ZipkinSpanResponse span2 =
        createSpanWithTags(
            "span2",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    spans.add(span1);
    spans.add(span2);

    Graph graph = graphBuilder.buildFromSpans(spans);

    // should only have one unique node
    assertThat(graph.nodes).hasSize(1);
    assertThat(graph.edges).isEmpty();
  }

  @Test
  void buildFromSpans_multipleChildrenSameParent_createsMultipleEdges() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse parentSpan =
        createSpanWithTags(
            "parent1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    ZipkinSpanResponse child1Span =
        createSpanWithTags(
            "child1",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app2",
                "kube.namespace", "ns2",
                "kube.operation", "op2",
                "resource", "res2"));

    ZipkinSpanResponse child2Span =
        createSpanWithTags(
            "child2",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app3",
                "kube.namespace", "ns3",
                "kube.operation", "op3",
                "resource", "res3"));

    spans.add(parentSpan);
    spans.add(child1Span);
    spans.add(child2Span);

    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(3);
    assertThat(graph.edges).hasSize(2);

    // verify both edges have the same parent
    Set<DependencyLink> edges = graph.edges;
    assertThat(edges.stream().allMatch(edge -> edge.parent().equals("app1:ns1:res1"))).isTrue();

    // verify different children
    Set<String> childIds = Set.of(edges.stream().map(DependencyLink::child).toArray(String[]::new));
    assertThat(childIds).containsExactlyInAnyOrder("app2:ns2:res2", "app3:ns3:res3");
  }

  @Test
  void buildFromSpans_duplicateEdges_deduplicatesEdges() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse parentSpan =
        createSpanWithTags(
            "parent1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    // two different child spans that reference the same parent, creating potential duplicate edges
    ZipkinSpanResponse child1Span =
        createSpanWithTags(
            "child1",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app2",
                "kube.namespace", "ns2",
                "kube.operation", "op2",
                "resource", "res2"));

    // second span with same child node ID but different span ID - should create duplicate edge
    ZipkinSpanResponse child2Span =
        createSpanWithTags(
            "child2",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app2",
                "kube.namespace", "ns2",
                "kube.operation", "op2",
                "resource", "res2"));

    spans.add(parentSpan);
    spans.add(child1Span);
    spans.add(child2Span);

    Graph graph = graphBuilder.buildFromSpans(spans);

    // should have 2 nodes (parent and child - child nodes are deduplicated)
    assertThat(graph.nodes).hasSize(2);

    // should have only 1 edge despite multiple spans creating the same parent-child relationship
    assertThat(graph.edges).hasSize(1);

    DependencyLink edge = graph.edges.iterator().next();
    assertThat(edge.parent()).isEqualTo("app1:ns1:res1");
    assertThat(edge.child()).isEqualTo("app2:ns2:res2");
  }

  @Test
  void buildFromSpans_complexHierarchy_buildsCorrectGraph() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    // root span
    ZipkinSpanResponse rootSpan =
        createSpanWithTags(
            "root",
            "trace1",
            null,
            Map.of(
                "kube.app", "root_app",
                "kube.namespace", "root_ns",
                "kube.operation", "root_op",
                "resource", "root_res"));

    // first level children
    ZipkinSpanResponse child1Span =
        createSpanWithTags(
            "child1",
            "trace1",
            "root",
            Map.of(
                "kube.app", "child1_app",
                "kube.namespace", "child1_ns",
                "kube.operation", "child1_op",
                "resource", "child1_res"));

    ZipkinSpanResponse child2Span =
        createSpanWithTags(
            "child2",
            "trace1",
            "root",
            Map.of(
                "kube.app", "child2_app",
                "kube.namespace", "child2_ns",
                "kube.operation", "child2_op",
                "resource", "child2_res"));

    // second level child
    ZipkinSpanResponse grandchildSpan =
        createSpanWithTags(
            "grandchild",
            "trace1",
            "child1",
            Map.of(
                "kube.app",
                "gc_app",
                "kube.namespace",
                "gc_ns",
                "kube.operation",
                "gc_op",
                "resource",
                "gc_res"));

    spans.add(rootSpan);
    spans.add(child1Span);
    spans.add(child2Span);
    spans.add(grandchildSpan);

    Graph graph = graphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(4);
    assertThat(graph.edges).hasSize(3);

    Set<DependencyLink> edges = graph.edges;

    // root -> child1
    assertThat(edges)
        .anyMatch(
            edge ->
                edge.parent().equals("root_app:root_ns:root_res")
                    && edge.child().equals("child1_app:child1_ns:child1_res"));

    // root -> child2
    assertThat(edges)
        .anyMatch(
            edge ->
                edge.parent().equals("root_app:root_ns:root_res")
                    && edge.child().equals("child2_app:child2_ns:child2_res"));

    // child1 -> grandchild
    assertThat(edges)
        .anyMatch(
            edge ->
                edge.parent().equals("child1_app:child1_ns:child1_res")
                    && edge.child().equals("gc_app:gc_ns:gc_res"));
  }

  private ZipkinSpanResponse createSpanWithTags(
      String id, String traceId, String parentId, Map<String, String> tags) {
    ZipkinSpanResponse span = new ZipkinSpanResponse(id, traceId);
    span.setParentId(parentId);
    span.setTags(new HashMap<>(tags));

    return span;
  }
}
