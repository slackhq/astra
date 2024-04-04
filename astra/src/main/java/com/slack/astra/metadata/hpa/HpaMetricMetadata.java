package com.slack.astra.metadata.hpa;

import com.slack.astra.metadata.core.AstraMetadata;
import com.slack.astra.proto.metadata.Metadata;

/**
 * HPA (horizontal pod autoscaler) metrics are calculated by the manager, and then stored in ZK so
 * that each node can individually locally their scaling metrics. This allows use of an HPA while
 * still centralizing the decision-making.
 */
public class HpaMetricMetadata extends AstraMetadata {
  public Metadata.HpaMetricMetadata.NodeRole nodeRole;
  public Double value;

  public HpaMetricMetadata(
      String name, Metadata.HpaMetricMetadata.NodeRole nodeRole, Double value) {
    super(name);
    this.nodeRole = nodeRole;
    this.value = value;
  }

  public Metadata.HpaMetricMetadata.NodeRole getNodeRole() {
    return nodeRole;
  }

  public void setNodeRole(Metadata.HpaMetricMetadata.NodeRole nodeRole) {
    this.nodeRole = nodeRole;
  }

  public Double getValue() {
    return value;
  }

  public void setValue(Double value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HpaMetricMetadata that)) return false;
    if (!super.equals(o)) return false;

    if (nodeRole != that.nodeRole) return false;
    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + nodeRole.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "HpaMetricMetadata{"
        + "nodeRole="
        + nodeRole
        + ", value="
        + value
        + ", name='"
        + name
        + '\''
        + '}';
  }
}
