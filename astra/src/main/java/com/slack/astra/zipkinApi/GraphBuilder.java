package com.slack.astra.zipkinApi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(GraphBuilder.class);

  public Graph buildFromSpans(List<ZipkinSpanResponse> spans) {
    Map<String, Node> nodes = new HashMap<>();
    Map<String, String> spanIdToNodeId = new HashMap<>();
    Map<String, List<String>> childrenByParentSpan = new HashMap<>();

    for (ZipkinSpanResponse span : spans) {
      if (span.getId() == null) {
        LOG.warn("Document={} cannot have missing id", span);
        continue;
      }

      Node childNode = createChildNodeFromSpan(span);
      nodes.putIfAbsent(childNode.getId(), childNode);
      spanIdToNodeId.put(span.getId(), childNode.getId());

      String parentId = span.getParentId();
      if (parentId != null) {
        childrenByParentSpan
            .computeIfAbsent(parentId, k -> new ArrayList<>())
            .add(childNode.getId());
      }
    }

    Set<DependencyLink> edges = buildEdges(nodes, childrenByParentSpan, spanIdToNodeId);

    return new Graph(new ArrayList<>(nodes.values()), edges);
  }

  private Node createChildNodeFromSpan(ZipkinSpanResponse span) {
    Map<String, String> tags = span.getTags();

    String app = tags.getOrDefault("kube.app", "unknown_app");
    String namespace = tags.getOrDefault("kube.namespace", "unknown_namespace");
    String operation = tags.getOrDefault("kube.operation", "unknown_operation");
    String resource = tags.getOrDefault("resource", "unknown_resource");

    // http.request type spans have the calling service's app/namespace, but the target service's
    // resource
    // so we must use the canonical_path to get the calling service's resource.
    if ("http.request".equals(operation) && tags.containsKey("tag.operation.canonical_path")) {
      resource = tags.get("tag.operation.canonical_path");
    }

    return new Node(app, namespace, operation, resource);
  }

  private Set<DependencyLink> buildEdges(
      Map<String, Node> nodes,
      Map<String, List<String>> childrenByParentSpan,
      Map<String, String> spanIdToNodeId) {
    Set<DependencyLink> edges = new HashSet<>();

    for (Map.Entry<String, List<String>> entry : childrenByParentSpan.entrySet()) {
      String parentSpanId = entry.getKey();
      String parentNodeId = spanIdToNodeId.get(parentSpanId);
      Node parentNode = parentNodeId != null ? nodes.get(parentNodeId) : null;

      for (String childNodeId : entry.getValue()) {
        Node childNode = nodes.get(childNodeId);

        if (parentNode != null && childNode != null) {
          edges.add(
              new DependencyLink.Builder()
                  .parent(parentNode.getId())
                  .child(childNode.getId())
                  .build());
        } else {
          LOG.warn(
              "Missing node for parentSpanId={} (parentNodeId={}) or childNodeId={}",
              parentSpanId,
              parentNodeId,
              childNodeId);
        }
      }
    }

    return edges;
  }
}
