package com.slack.astra.zipkinApi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class NodeTest {
  @Test
  void constructor_validParameters_createsNode() {
    Node node = new Node("app", "namespace", "operation", "resource");

    assertThat(node.getId()).isEqualTo("app:namespace:resource");
    assertThat(node.getApp()).isEqualTo("app");
    assertThat(node.getNamespace()).isEqualTo("namespace");
    assertThat(node.getOperation()).isEqualTo("operation");
    assertThat(node.getResource()).isEqualTo("resource");
  }

  @Test
  void constructor_withNullApp_throwsNullPointerException() {
    assertThatThrownBy(() -> new Node(null, "namespace", "operation", "resource"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("app == null");
  }

  @Test
  void constructor_withNullNamespace_throwsNullPointerException() {
    assertThatThrownBy(() -> new Node("app", null, "operation", "resource"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("namespace == null");
  }

  @Test
  void constructor_withNullOperation_throwsNullPointerException() {
    assertThatThrownBy(() -> new Node("app", "namespace", null, "resource"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("operation == null");
  }

  @Test
  void constructor_withNullResource_throwsNullPointerException() {
    assertThatThrownBy(() -> new Node("app", "namespace", "operation", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("resource == null");
  }

  @Test
  void equals_sameObject_returnsTrue() {
    Node node = new Node("app", "namespace", "operation", "resource");

    assertThat(node.equals(node)).isTrue();
  }

  @Test
  void equals_differentType_returnsFalse() {
    Node node = new Node("app", "namespace", "operation", "resource");

    assertThat(node.equals("not a node")).isFalse();
  }

  @Test
  void equals_sameValues_returnsTrue() {
    Node node1 = new Node("app", "namespace", "operation", "resource");
    Node node2 = new Node("app", "namespace", "operation", "resource");

    assertThat(node1).isEqualTo(node2);
  }

  @Test
  void equals_differentApp_returnsFalse() {
    Node node1 = new Node("app1", "namespace", "operation", "resource");
    Node node2 = new Node("app2", "namespace", "operation", "resource");

    assertThat(node1).isNotEqualTo(node2);
  }

  @Test
  void equals_differentNamespace_returnsFalse() {
    Node node1 = new Node("app", "namespace1", "operation", "resource");
    Node node2 = new Node("app", "namespace2", "operation", "resource");

    assertThat(node1).isNotEqualTo(node2);
  }

  @Test
  void equals_differentOperation_returnsFalse() {
    Node node1 = new Node("app", "namespace", "operation1", "resource");
    Node node2 = new Node("app", "namespace", "operation2", "resource");

    assertThat(node1).isNotEqualTo(node2);
  }

  @Test
  void equals_differentResource_returnsFalse() {
    Node node1 = new Node("app", "namespace", "operation", "resource1");
    Node node2 = new Node("app", "namespace", "operation", "resource2");

    assertThat(node1).isNotEqualTo(node2);
  }

  @Test
  void hashCode_sameValues_returnsSameHashCode() {
    Node node1 = new Node("app", "namespace", "operation", "resource");
    Node node2 = new Node("app", "namespace", "operation", "resource");

    assertThat(node1.hashCode()).isEqualTo(node2.hashCode());
  }
}
