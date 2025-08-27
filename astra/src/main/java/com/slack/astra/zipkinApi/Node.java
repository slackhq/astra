package com.slack.astra.zipkinApi;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Node {
  private final String id;
  private final String serviceName;
  private String app;
  private String namespace;
  private String operation;
  private String resource;

  public Node(String id, String serviceName) {
    this.id = id;
    this.serviceName = serviceName;
  }

  public String getId() {
    return this.id;
  }

  public String getServiceName() {
    return this.serviceName;
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

  public void setApp(String app) {
    this.app = app;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Node)) return false;
    Node that = (Node) o;
    return this.id.equals(that.getId())
        && this.serviceName.equals(that.getServiceName())
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
