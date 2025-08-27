package com.slack.astra.zipkinApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Graph {
  List<Node> nodes;
  Set<DependencyLink> edges;

  public Graph(ArrayList<Node> nodes, Set<DependencyLink> edges) {
    this.nodes = nodes;
    this.edges = edges;
  }
}
