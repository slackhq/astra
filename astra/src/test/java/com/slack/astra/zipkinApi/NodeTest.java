package com.slack.astra.zipkinApi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import java.util.Collections;
import org.junit.jupiter.api.Test;

public class NodeTest {
  @Test
  void constructor_validParameters_createsNode() {
    Node node = new Node("id", "service");

    assertThat(node.getId()).isEqualTo("id");
    assertThat(node.getServiceName()).isEqualTo("service");
    assertThat(node.getApp()).isNull();
    assertThat(node.getNamespace()).isNull();
    assertThat(node.getOperation()).isNull();
    assertThat(node.getResource()).isNull();
    assertThat(node.getChildren()).isEqualTo(Collections.emptyList());
  }

  @Test
  void setApp_validValue_setsApp() {
    Node node = new Node("id", "service");
    node.setApp("testApp");

    assertThat(node.getApp()).isEqualTo("testApp");
  }

  @Test
  void setApp_nullValue_setsAppToNull() {
    Node node = new Node("id", "service");
    node.setApp("testApp");
    node.setApp(null);

    assertThat(node.getApp()).isNull();
  }

  @Test
  void setNamespace_validValue_setsNamespace() {
    Node node = new Node("id", "service");
    node.setNamespace("testNamespace");

    assertThat(node.getNamespace()).isEqualTo("testNamespace");
  }

  @Test
  void setOperation_validValue_setsOperation() {
    Node node = new Node("id", "service");
    node.setOperation("testOperation");

    assertThat(node.getOperation()).isEqualTo("testOperation");
  }

  @Test
  void setResource_validValue_setsResource() {
    Node node = new Node("id", "service");
    node.setResource("testResource");

    assertThat(node.getResource()).isEqualTo("testResource");
  }

  @Test
  void addChild_nullNotAllowed() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(
            () -> {
              Node a = new Node("a", "service1");
              a.addChild(null);
            })
        .withMessage("child == null");
  }

  @Test
  void addChild_selfNotAllowed() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () -> {
              Node a = new Node("a", "service1");
              a.addChild(a);
            })
        .withMessageContaining("circular dependency on");
  }

  @Test
  void addChild_multipleChildren_addsAllChildren() {
    Node parent = new Node("parent", "parentService");
    Node child1 = new Node("child1", "childService1");
    Node child2 = new Node("child2", "childService2");

    parent.addChild(child1).addChild(child2);

    assertThat(parent.getChildren()).containsExactly(child1, child2);
  }

  @Test
  void addChild_duplicateChild_addsOnce() {
    Node parent = new Node("parent", "parentService");
    Node child = new Node("child", "childService");

    parent.addChild(child).addChild(child);

    assertThat(parent.getChildren()).containsExactly(child);
  }

  @Test
  void equals_sameObject_returnsTrue() {
    Node node = new Node("id", "service");

    assertThat(node.equals(node)).isTrue();
  }

  @Test
  void equals_differentType_returnsFalse() {
    Node node = new Node("id", "service");

    assertThat(node.equals("not a node")).isFalse();
  }

  @Test
  void equals_sameIdAndService_returnsTrue() {
    Node node1 = new Node("id", "service");
    Node node2 = new Node("id", "service");

    assertThat(node1).isEqualTo(node2);
  }

  @Test
  void equals_differentId_returnsFalse() {
    Node node1 = new Node("id1", "service");
    Node node2 = new Node("id2", "service");

    assertThat(node1).isNotEqualTo(node2);
  }

  @Test
  void equals_differentService_returnsFalse() {
    Node node1 = new Node("id", "service1");
    Node node2 = new Node("id", "service2");

    assertThat(node1).isNotEqualTo(node2);
  }

  @Test
  void equals_sameIdAndServiceDifferentOptionalFields_returnsFalse() {
    Node node1 = new Node("id", "service");
    node1.setApp("app1");
    node1.setNamespace("namespace1");
    node1.setOperation("operation1");
    node1.setResource("resource1");

    Node node2 = new Node("id", "service");
    node2.setApp("app2");
    node2.setNamespace("namespace2");
    node2.setOperation("operation2");
    node2.setResource("resource2");

    assertThat(node1).isNotEqualTo(node2);
  }

  @Test
  void equals_sameIdServiceAndOptionalFields_returnsTrue() {
    Node node1 = new Node("id", "service");
    node1.setApp("app");
    node1.setNamespace("namespace");
    node1.setOperation("operation");
    node1.setResource("resource");

    Node node2 = new Node("id", "service");
    node2.setApp("app");
    node2.setNamespace("namespace");
    node2.setOperation("operation");
    node2.setResource("resource");

    assertThat(node1).isEqualTo(node2);
  }

  @Test
  void equals_sameIdAndServiceDifferentChildren_returnsTrue() {
    Node parent1 = new Node("parent", "parentService");
    Node parent2 = new Node("parent", "parentService");
    Node child = new Node("child", "childService");

    parent1.addChild(child);

    assertThat(parent1).isEqualTo(parent2);
  }
}
