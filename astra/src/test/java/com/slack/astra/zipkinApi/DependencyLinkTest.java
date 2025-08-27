package com.slack.astra.zipkinApi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

public class DependencyLinkTest {

  @Test
  void builder_parentNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new DependencyLink.Builder().parent(null))
        .withMessage("parent == null");
  }

  @Test
  void builder_childNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new DependencyLink.Builder().child(null))
        .withMessage("child == null");
  }

  @Test
  void build_missingParent_throwsIllegalStateException() {
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> new DependencyLink.Builder().child("child").build())
        .withMessage("Missing : parent");
  }

  @Test
  void build_missingChild_throwsIllegalStateException() {
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> new DependencyLink.Builder().parent("parent").build())
        .withMessage("Missing : child");
  }

  @Test
  void build_withValidParentAndChild_succeeds() {
    DependencyLink link = new DependencyLink.Builder().parent("parent").child("child").build();

    assertThat(link.parent).isEqualTo("parent");
    assertThat(link.child).isEqualTo("child");
  }

  @Test
  void builderConstructor_copiesFromSourceDependencyLink() {
    DependencyLink original = new DependencyLink.Builder().parent("parent").child("child").build();
    DependencyLink copy = new DependencyLink.Builder(original).build();

    assertThat(copy.parent).isEqualTo("parent");
    assertThat(copy.child).isEqualTo("child");
    assertThat(copy).isEqualTo(original);
  }

  @Test
  void equals_sameObject_returnsTrue() {
    DependencyLink link = new DependencyLink.Builder().parent("parent").child("child").build();
    assertThat(link.equals(link)).isTrue();
  }

  @Test
  void equals_differentType_returnsFalse() {
    DependencyLink link = new DependencyLink.Builder().parent("parent").child("child").build();
    assertThat(link.equals(123)).isFalse();
  }

  @Test
  void equals_sameParentAndChild_returnsTrue() {
    DependencyLink link1 = new DependencyLink.Builder().parent("parent").child("child").build();
    DependencyLink link2 = new DependencyLink.Builder().parent("parent").child("child").build();

    assertThat(link1).isEqualTo(link2);
  }

  @Test
  void equals_differentParent_returnsFalse() {
    DependencyLink link1 = new DependencyLink.Builder().parent("parent1").child("child").build();
    DependencyLink link2 = new DependencyLink.Builder().parent("parent2").child("child").build();

    assertThat(link1).isNotEqualTo(link2);
  }

  @Test
  void equals_differentChild_returnsFalse() {
    DependencyLink link1 = new DependencyLink.Builder().parent("parent").child("child1").build();
    DependencyLink link2 = new DependencyLink.Builder().parent("parent").child("child2").build();

    assertThat(link1).isNotEqualTo(link2);
  }

  @Test
  void builder_overwriteParent_usesLatestValue() {
    DependencyLink link =
        new DependencyLink.Builder().parent("parent1").parent("parent2").child("child").build();

    assertThat(link.parent).isEqualTo("parent2");
  }

  @Test
  void builder_overwriteChild_usesLatestValue() {
    DependencyLink link =
        new DependencyLink.Builder().parent("parent").child("child1").child("child2").build();

    assertThat(link.child).isEqualTo("child2");
  }
}
