package com.slack.astra.zipkinApi;

public class Node {
  private final String id;
  private final String app;
  private final String namespace;
  private final String operation;
  private final String resource;

  public Node(String app, String namespace, String operation, String resource) {
    if (app == null) throw new NullPointerException("app == null");
    if (namespace == null) throw new NullPointerException("namespace == null");
    if (operation == null) throw new NullPointerException("operation == null");
    if (resource == null) throw new NullPointerException("resource == null");

    this.app = app;
    this.namespace = namespace;
    this.operation = operation;
    this.resource = resource;

    this.id = this.app + ":" + this.namespace + ":" + this.resource;
  }

  public String getId() {
    return this.id;
  }

  public String getApp() {
    return this.app;
  }

  public String getNamespace() {
    return this.namespace;
  }

  public String getOperation() {
    return this.operation;
  }

  public String getResource() {
    return this.resource;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Node)) return false;
    Node that = (Node) o;
    return this.id.equals(that.getId())
        && java.util.Objects.equals(this.app, that.getApp())
        && java.util.Objects.equals(this.namespace, that.getNamespace())
        && java.util.Objects.equals(this.operation, that.getOperation())
        && java.util.Objects.equals(this.resource, that.getResource());
  }

  @Override
  public int hashCode() {
    int result = 1;
    result *= 1000003;
    result ^= id.hashCode();

    return result;
  }
}
