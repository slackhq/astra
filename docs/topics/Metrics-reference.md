# Metrics reference

Reference of available Prometheus metrics and common labels.

## Prometheus metric types
Astra exposes metrics using one of the three following Prometheus types.

<deflist>
    <def title="counter">
        Monotonically increasing counter, which can only increase or be reset to zero.
    </def>
    <def title="gauge">
        Single numerical value that can go up or down.
    </def>
    <def title="summary">
        Provides total count and sum of observation, along with configurable quantiles.
        <tip>The most common quantile values include<code>0.0, 0.5, 0.75, 0.9, 0.95, 0.98, 0.99, 0.999,</code> and <code>1.0</code></tip>
        <tip>
            For each summary up to three series will be exposed:
            <code-block>&lt;metric_name&gt;{quantile="&lt;quantile&gt;"}</code-block>
            <code-block>&lt;metric_name&gt;_sum</code-block>
            <code-block>&lt;metric_name&gt;_count</code-block>
        </tip>
    </def>
    
</deflist>

## Common labels

Labels automatically applied to all exported Prometheus metrics.
<deflist type="full">
    <snippet id="common-configs">
        <def title="astra_cluster_name">
            Cluster name, as defined in <a href="Config-options.md#clusterconfig"><path>clusterConfig.clusterName</path>.</a>
        </def>
        <def title="astra_component">
            Node type, one of valid <a href="Config-options.md#noderoles">node roles.</a>
            <code-block>
                query, index, cache, manager, recovery, preprocessor
            </code-block>
        </def>
        <def title="astra_env">
            Cluster environment, as defined in <a href="Config-options.md#clusterconfig"><path>clusterConfig.env</path>.</a>
        </def>
    </snippet>
</deflist>

## Astra metrics

<deflist type="full" sorted="asc">
      <def title="astra_index_commits_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_index_commits_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_index_final_merges_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_index_final_merges_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_index_merge_count_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_index_merge_stall_threads | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_index_merge_stall_time_ms_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_index_refreshes_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_index_refreshes_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_preprocessor_bulk_ingest_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_preprocessor_bulk_ingest_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_preprocessor_incoming_byte_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="astra_preprocessor_incoming_docs_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="bulk_ingest_producer_batch_size | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="bulk_ingest_producer_failed_set_response_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="bulk_ingest_producer_kafka_restart_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="bulk_ingest_producer_kafka_restart_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="bulk_ingest_producer_stall_counter_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
   <def title="cached_cache_slots_size | gauge">
        The amount of cache slot znodes stored in Zookeeper.
        <deflist type="full" collapsible="true">
            <def title="labels">
                <deflist type="full" collapsible="true">
                    <def title="cacheSlotState">
                        The current state of the cache slot.
                        <code-block>
                            FREE, ASSIGNED, LOADING, LIVE, EVICT, EVICTING, UNRECOGNIZED
                        </code-block>
                    </def> 
                    <include from="Metrics-reference.md" element-id="common-configs" /> 
                </deflist>
            </def>
        </deflist>
    </def>
  <def title="cached_recovery_nodes_size | gauge">
        The amount of recovery node znodes stored in Zookeeper.
        <deflist type="full" collapsible="true">
            <def title="labels">
                <deflist>
                    <include from="Metrics-reference.md" element-id="common-configs" />
                </deflist>
            </def>
        </deflist>
    </def>
 <def title="cached_recovery_tasks_size | gauge">
        The amount of recovery task znodes stored in Zookeeper.
        <deflist type="full" collapsible="true">
            <def title="labels">
                <deflist>
                    <include from="Metrics-reference.md" element-id="common-configs" />
                </deflist>
            </def>
        </deflist>
    </def>
  <def title="cached_replica_nodes_size | gauge">
        The amount of replica znodes stored in Zookeeper.
        <deflist type="full" collapsible="true">
            <def title="labels">
                <deflist>
                    <include from="Metrics-reference.md" element-id="common-configs" />
                </deflist>
            </def>
        </deflist>
    </def>
    <def title="cached_service_nodes_size | gauge">
        The amount of dataset znodes stored in Zookeeper.
        <deflist type="full" collapsible="true">
            <def title="labels">
                <deflist>
                    <include from="Metrics-reference.md" element-id="common-configs" />
                </deflist>
            </def>
        </deflist>
    </def>
  <def title="cached_snapshots_size | gauge">
        The amount of snapshost znodes stored in Zookeeper.
        <deflist type="full" collapsible="true">
            <def title="labels">
                <deflist>
                    <include from="Metrics-reference.md" element-id="common-configs" />
                </deflist>
            </def>
        </deflist>
    </def>
  <def title="chunk_assignment_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="successful"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="chunk_assignment_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="successful"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="chunk_eviction_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="successful"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="chunk_eviction_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="successful"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="convert_and_duplicate_field_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="convert_errors_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="convert_field_value_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="distributed_query_apdex_frustrated_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="distributed_query_apdex_satisfied_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="distributed_query_apdex_tolerating_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="distributed_query_snapshots_with_replicas_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="distributed_query_total_snapshots_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="dropped_fields_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="hpa_cache_demand_factor_rep1 | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="hpa_cache_demand_factor_rep2 | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="index_files_upload_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="index_files_upload_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="live_bytes_dir | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="live_bytes_indexed | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="live_messages_indexed | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="messages_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="messages_received_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="metadata_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="preprocessor_dataset_rate_limit_reload_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="preprocessor_dataset_rate_limit_reload_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="preprocessor_rate_limit_bytes_dropped_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="reason"></def><def title="service"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="preprocessor_rate_limit_messages_dropped_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="reason"></def><def title="service"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="records_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="records_received_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="recovery_task_assignment_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="recovery_task_assignment_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="recovery_tasks_assigned_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="recovery_tasks_assignment_failures_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="recovery_tasks_created_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="partitionId"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="recovery_tasks_insufficient_capacity_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_assign_available_capacity | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_assign_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_assign_pending | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_assign_succeeded_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_assign_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_assign_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_assignment_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_assignment_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_delete_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_delete_success_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_delete_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_delete_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_mark_evict_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_mark_evict_succeeded_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_mark_evict_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replica_mark_evict_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replicas_created_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="replicas_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="replicaSet"></def>
<include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="rollover_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="rollover_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="rollovers_completed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="rollovers_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="rollovers_initiated_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="search_metadata_total_change_counter_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="snapshot_delete_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="snapshot_delete_success_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="snapshot_delete_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="snapshot_delete_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="snapshot_timer_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="snapshot_timer_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="stale_snapshot_delete_failed_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="stale_snapshot_delete_success_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="total_fields_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>

</deflist>

## Armeria metrics
<deflist>
  <def title="armeria_server_file_vfs_cache_eviction_weight_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="vfs"></def><def title="route"></def><def title="hostname_pattern"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_executor_queue_remaining_tasks | gauge">
    The number of additional elements that this queue can ideally accept without blocking
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="name"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_executor_pool_size_threads | gauge">
    The current number of threads in the pool
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="name"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_build_info | gauge">
    A metric with a constant '1' value labeled by version and commit hash from which Armeria was built.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="commit"></def><def title="repo_status"></def><def title="version"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_file_vfs_cache_requests_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="result"></def><def title="vfs"></def><def title="route"></def><def title="hostname_pattern"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_netty_common_event_loop_pending_tasks | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_executor_completed_tasks_total | counter">
    The approximate total number of tasks that have completed execution
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="name"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_router_virtual_host_cache_estimated_size | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_pending_responses | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_executor_queued_tasks | gauge">
    The approximate number of tasks that are queued for execution
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="name"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_router_virtual_host_cache_evictions_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_router_virtual_host_cache_requests_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="result"></def><def title="hostname_pattern"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_connections | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_netty_common_event_loop_workers | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_router_virtual_host_cache_eviction_weight_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_executor_pool_max_threads | gauge">
    The maximum allowed number of threads in the pool
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="name"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_file_vfs_cache_evictions_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="vfs"></def><def title="route"></def><def title="hostname_pattern"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_exceptions_unhandled_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_connections_lifespan_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="protocol"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_connections_lifespan_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="protocol"></def><def title="quantile"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_executor_active_threads | gauge">
    The approximate number of threads that are actively executing tasks
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="name"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_executor_pool_core_threads | gauge">
    The core number of threads for the pool
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="name"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="armeria_server_file_vfs_cache_estimated_size | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="vfs"></def><def title="route"></def><def title="hostname_pattern"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
</deflist>

## Kafka metrics
<deflist type="full" sorted="asc">
  <def title="kafka_app_info_start_time_ms | gauge">
    Metric indicating start-time-ms
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_commit_sync_time_ns_total | counter">
    The total time the consumer has spent in commitSync in nanoseconds
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_committed_time_ns_total | counter">
    The total time the consumer has spent in committed in nanoseconds
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_connection_close_rate | gauge">
    The number of connections closed per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_connection_close_total | counter">
    The total number of connections closed
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_connection_count | gauge">
    The current number of active connections.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_connection_creation_rate | gauge">
    The number of new connections established per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_connection_creation_total | counter">
    The total number of new connections established
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_assigned_partitions | gauge">
    The number of partitions currently assigned to this consumer
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_commit_latency_avg | gauge">
    The average time taken for a commit request
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_commit_latency_max | gauge">
    The max time taken for a commit request
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_commit_rate | gauge">
    The number of commit calls per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_commit_total | counter">
    The total number of commit calls
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_failed_rebalance_rate_per_hour | gauge">
    The number of failed rebalance events per hour
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_failed_rebalance_total | counter">
    The total number of failed rebalance events
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_heartbeat_rate | gauge">
    The number of heartbeats per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_heartbeat_response_time_max | gauge">
    The max time taken to receive a response to a heartbeat request
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_heartbeat_total | counter">
    The total number of heartbeats
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_join_rate | gauge">
    The number of group joins per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_join_time_avg | gauge">
    The average time taken for a group rejoin
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_join_time_max | gauge">
    The max time taken for a group rejoin
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_join_total | counter">
    The total number of group joins
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_last_heartbeat_seconds_ago | gauge">
    The number of seconds since the last coordinator heartbeat was sent
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_last_rebalance_seconds_ago | gauge">
    The number of seconds since the last successful rebalance event
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_partition_assigned_latency_avg | gauge">
    The average time taken for a partition-assigned rebalance listener callback
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_partition_assigned_latency_max | gauge">
    The max time taken for a partition-assigned rebalance listener callback
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_partition_lost_latency_avg | gauge">
    The average time taken for a partition-lost rebalance listener callback
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_partition_lost_latency_max | gauge">
    The max time taken for a partition-lost rebalance listener callback
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_partition_revoked_latency_avg | gauge">
    The average time taken for a partition-revoked rebalance listener callback
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_partition_revoked_latency_max | gauge">
    The max time taken for a partition-revoked rebalance listener callback
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_rebalance_latency_avg | gauge">
    The average time taken for a group to complete a successful rebalance, which may be composed of several failed re-trials until it succeeded
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_rebalance_latency_max | gauge">
    The max time taken for a group to complete a successful rebalance, which may be composed of several failed re-trials until it succeeded
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_rebalance_latency_total | counter">
    The total number of milliseconds this consumer has spent in successful rebalances since creation
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_rebalance_rate_per_hour | gauge">
    The number of successful rebalance events per hour, each event is composed of several failed re-trials until it succeeded
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_rebalance_total | counter">
    The total number of successful rebalance events, each event is composed of several failed re-trials until it succeeded
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_sync_rate | gauge">
    The number of group syncs per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_sync_time_avg | gauge">
    The average time taken for a group sync
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_sync_time_max | gauge">
    The max time taken for a group sync
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_coordinator_sync_total | counter">
    The total number of group syncs
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_failed_authentication_rate | gauge">
    The number of connections with failed authentication per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_failed_authentication_total | counter">
    The total number of connections with failed authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_failed_reauthentication_rate | gauge">
    The number of failed re-authentication of connections per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_failed_reauthentication_total | counter">
    The total number of failed re-authentication of connections
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_bytes_consumed_rate | gauge">
    The average number of bytes consumed per second for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_bytes_consumed_total | counter">
    The total number of bytes consumed for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_fetch_latency_avg | gauge">
    The average time taken for a fetch request.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_fetch_latency_max | gauge">
    The max time taken for any fetch request.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_fetch_rate | gauge">
    The number of fetch requests per second.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_fetch_size_avg | gauge">
    The average number of bytes fetched per request for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_fetch_size_max | gauge">
    The maximum number of bytes fetched per request for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_fetch_throttle_time_avg | gauge">
    The average throttle time in ms
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_fetch_throttle_time_max | gauge">
    The maximum throttle time in ms
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_fetch_total | counter">
    The total number of fetch requests.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_preferred_read_replica | gauge">
    The current read replica for the partition, or -1 if reading from leader
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="partition"></def><def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_records_consumed_rate | gauge">
    The average number of records consumed per second for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_records_consumed_total | counter">
    The total number of records consumed for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_records_lag | gauge">
    The latest lag of the partition
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="partition"></def><def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_records_lag_avg | gauge">
    The average lag of the partition
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="partition"></def><def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_records_lag_max | gauge">
    The max lag of the partition
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="partition"></def><def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_records_lead | gauge">
    The latest lead of the partition
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="partition"></def><def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_records_lead_avg | gauge">
    The average lead of the partition
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="partition"></def><def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_records_lead_min | gauge">
    The min lead of the partition
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="partition"></def><def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_fetch_manager_records_per_request_avg | gauge">
    The average number of records in each request for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_incoming_byte_rate | gauge">
    The number of bytes read off all sockets per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_incoming_byte_total | counter">
    The total number of bytes read off all sockets
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_io_ratio | gauge">
    *Deprecated* The fraction of time the I/O thread spent doing I/O
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_io_time_ns_avg | gauge">
    The average length of time for I/O per select call in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_io_time_ns_total | counter">
    The total time the I/O thread spent doing I/O
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_io_wait_ratio | gauge">
    *Deprecated* The fraction of time the I/O thread spent waiting
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_io_wait_time_ns_avg | gauge">
    The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_io_wait_time_ns_total | counter">
    The total time the I/O thread spent waiting
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_io_waittime_total | counter">
    *Deprecated* The total time the I/O thread spent waiting
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_iotime_total | counter">
    *Deprecated* The total time the I/O thread spent doing I/O
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_last_poll_seconds_ago | gauge">
    The number of seconds since the last poll() invocation.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_network_io_rate | gauge">
    The number of network operations (reads or writes) on all connections per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_network_io_total | counter">
    The total number of network operations (reads or writes) on all connections
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_incoming_byte_rate | gauge">
    The number of incoming bytes per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_incoming_byte_total | counter">
    The total number of incoming bytes
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_outgoing_byte_rate | gauge">
    The number of outgoing bytes per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_outgoing_byte_total | counter">
    The total number of outgoing bytes
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_request_latency_avg | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_request_latency_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_request_rate | gauge">
    The number of requests sent per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_request_size_avg | gauge">
    The average size of requests sent.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_request_size_max | gauge">
    The maximum size of any request sent.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_request_total | counter">
    The total number of requests sent
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_response_rate | gauge">
    The number of responses received per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_node_response_total | counter">
    The total number of responses received
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_outgoing_byte_rate | gauge">
    The number of outgoing bytes sent to all servers per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_outgoing_byte_total | counter">
    The total number of outgoing bytes sent to all servers
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_poll_idle_ratio_avg | gauge">
    The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_reauthentication_latency_avg | gauge">
    The average latency observed due to re-authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_reauthentication_latency_max | gauge">
    The max latency observed due to re-authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_request_rate | gauge">
    The number of requests sent per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_request_size_avg | gauge">
    The average size of requests sent.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_request_size_max | gauge">
    The maximum size of any request sent.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_request_total | counter">
    The total number of requests sent
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_response_rate | gauge">
    The number of responses received per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_response_total | counter">
    The total number of responses received
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_select_rate | gauge">
    The number of times the I/O layer checked for new I/O to perform per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_select_total | counter">
    The total number of times the I/O layer checked for new I/O to perform
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_successful_authentication_no_reauth_total | counter">
    The total number of connections with successful authentication where the client does not support re-authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_successful_authentication_rate | gauge">
    The number of connections with successful authentication per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_successful_authentication_total | counter">
    The total number of connections with successful authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_successful_reauthentication_rate | gauge">
    The number of successful re-authentication of connections per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_successful_reauthentication_total | counter">
    The total number of successful re-authentication of connections
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_time_between_poll_avg | gauge">
    The average delay between invocations of poll() in milliseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_consumer_time_between_poll_max | gauge">
    The max delay between invocations of poll() in milliseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_batch_size_avg | gauge">
    The average number of bytes sent per partition per-request.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_batch_size_max | gauge">
    The max number of bytes sent per partition per-request.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_batch_split_rate | gauge">
    The average number of batch splits per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_batch_split_total | counter">
    The total number of batch splits
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_buffer_available_bytes | gauge">
    The total amount of buffer memory that is not being used (either unallocated or in the free list).
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_buffer_exhausted_rate | gauge">
    The average per-second number of record sends that are dropped due to buffer exhaustion
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_buffer_exhausted_total | counter">
    The total number of record sends that are dropped due to buffer exhaustion
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_buffer_total_bytes | gauge">
    The maximum amount of buffer memory the client can use (whether or not it is currently used).
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_bufferpool_wait_ratio | gauge">
    The fraction of time an appender waits for space allocation.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_bufferpool_wait_time_ns_total | counter">
    The total time in nanoseconds an appender waits for space allocation.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_bufferpool_wait_time_total | counter">
    *Deprecated* The total time an appender waits for space allocation.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_compression_rate_avg | gauge">
    The average compression rate of record batches, defined as the average ratio of the compressed batch size over the uncompressed size.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_connection_close_rate | gauge">
    The number of connections closed per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_connection_close_total | counter">
    The total number of connections closed
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_connection_count | gauge">
    The current number of active connections.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_connection_creation_rate | gauge">
    The number of new connections established per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_connection_creation_total | counter">
    The total number of new connections established
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_failed_authentication_rate | gauge">
    The number of connections with failed authentication per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_failed_authentication_total | counter">
    The total number of connections with failed authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_failed_reauthentication_rate | gauge">
    The number of failed re-authentication of connections per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_failed_reauthentication_total | counter">
    The total number of failed re-authentication of connections
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_flush_time_ns_total | counter">
    Total time producer has spent in flush in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_incoming_byte_rate | gauge">
    The number of bytes read off all sockets per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_incoming_byte_total | counter">
    The total number of bytes read off all sockets
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_io_ratio | gauge">
    *Deprecated* The fraction of time the I/O thread spent doing I/O
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_io_time_ns_avg | gauge">
    The average length of time for I/O per select call in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_io_time_ns_total | counter">
    The total time the I/O thread spent doing I/O
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_io_wait_ratio | gauge">
    *Deprecated* The fraction of time the I/O thread spent waiting
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_io_wait_time_ns_avg | gauge">
    The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_io_wait_time_ns_total | counter">
    The total time the I/O thread spent waiting
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_io_waittime_total | counter">
    *Deprecated* The total time the I/O thread spent waiting
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_iotime_total | counter">
    *Deprecated* The total time the I/O thread spent doing I/O
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_metadata_age | gauge">
    The age in seconds of the current producer metadata being used.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_metadata_wait_time_ns_total | counter">
    Total time producer has spent waiting on topic metadata in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_network_io_rate | gauge">
    The number of network operations (reads or writes) on all connections per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_network_io_total | counter">
    The total number of network operations (reads or writes) on all connections
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_incoming_byte_rate | gauge">
    The number of incoming bytes per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_incoming_byte_total | counter">
    The total number of incoming bytes
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_outgoing_byte_rate | gauge">
    The number of outgoing bytes per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_outgoing_byte_total | counter">
    The total number of outgoing bytes
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_request_latency_avg | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_request_latency_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_request_rate | gauge">
    The number of requests sent per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_request_size_avg | gauge">
    The average size of requests sent.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_request_size_max | gauge">
    The maximum size of any request sent.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_request_total | counter">
    The total number of requests sent
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_response_rate | gauge">
    The number of responses received per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_node_response_total | counter">
    The total number of responses received
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def><def title="node_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_outgoing_byte_rate | gauge">
    The number of outgoing bytes sent to all servers per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_outgoing_byte_total | counter">
    The total number of outgoing bytes sent to all servers
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_produce_throttle_time_avg | gauge">
    The average time in ms a request was throttled by a broker
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_produce_throttle_time_max | gauge">
    The maximum time in ms a request was throttled by a broker
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_reauthentication_latency_avg | gauge">
    The average latency observed due to re-authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_reauthentication_latency_max | gauge">
    The max latency observed due to re-authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_error_rate | gauge">
    The average per-second number of record sends that resulted in errors
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_error_total | counter">
    The total number of record sends that resulted in errors
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_queue_time_avg | gauge">
    The average time in ms record batches spent in the send buffer.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_queue_time_max | gauge">
    The maximum time in ms record batches spent in the send buffer.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_retry_rate | gauge">
    The average per-second number of retried record sends
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_retry_total | counter">
    The total number of retried record sends
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_send_rate | gauge">
    The average number of records sent per second.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_send_total | counter">
    The total number of records sent.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_size_avg | gauge">
    The average record size
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_record_size_max | gauge">
    The maximum record size
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_records_per_request_avg | gauge">
    The average number of records per request.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_request_latency_avg | gauge">
    The average request latency in ms
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_request_latency_max | gauge">
    The maximum request latency in ms
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_request_rate | gauge">
    The number of requests sent per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_request_size_avg | gauge">
    The average size of requests sent.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_request_size_max | gauge">
    The maximum size of any request sent.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_request_total | counter">
    The total number of requests sent
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_requests_in_flight | gauge">
    The current number of in-flight requests awaiting a response.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_response_rate | gauge">
    The number of responses received per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_response_total | counter">
    The total number of responses received
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_select_rate | gauge">
    The number of times the I/O layer checked for new I/O to perform per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_select_total | counter">
    The total number of times the I/O layer checked for new I/O to perform
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_successful_authentication_no_reauth_total | counter">
    The total number of connections with successful authentication where the client does not support re-authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_successful_authentication_rate | gauge">
    The number of connections with successful authentication per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_successful_authentication_total | counter">
    The total number of connections with successful authentication
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_successful_reauthentication_rate | gauge">
    The number of successful re-authentication of connections per second
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_successful_reauthentication_total | counter">
    The total number of successful re-authentication of connections
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_topic_byte_rate | gauge">
    The average number of bytes sent per second for a topic.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_topic_byte_total | counter">
    The total number of bytes sent for a topic.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_topic_compression_rate | gauge">
    The average compression rate of record batches for a topic, defined as the average ratio of the compressed batch size over the uncompressed size.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_topic_record_error_rate | gauge">
    The average per-second number of record sends that resulted in errors for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_topic_record_error_total | counter">
    The total number of record sends that resulted in errors for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_topic_record_retry_rate | gauge">
    The average per-second number of retried record sends for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_topic_record_retry_total | counter">
    The total number of retried record sends for a topic
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_topic_record_send_rate | gauge">
    The average number of records sent per second for a topic.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_topic_record_send_total | counter">
    The total number of records sent for a topic.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="topic"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_txn_abort_time_ns_total | counter">
    Total time producer has spent in abortTransaction in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_txn_begin_time_ns_total | counter">
    Total time producer has spent in beginTransaction in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_txn_commit_time_ns_total | counter">
    Total time producer has spent in commitTransaction in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_txn_init_time_ns_total | counter">
    Total time producer has spent in initTransactions in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_txn_send_offsets_time_ns_total | counter">
    Total time producer has spent in sendOffsetsToTransaction in nanoseconds.
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="kafka_producer_waiting_threads | gauge">
    The number of user threads blocked waiting for buffer memory to enqueue their records
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="kafka_version"></def><def title="client_id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
</deflist>

## GRPC metrics
<deflist  type="full" sorted="asc">
  <def title="grpc_service_active_requests | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_request_duration_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="quantile"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_request_duration_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_request_length | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="quantile"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_request_length_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_requests_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="result"></def><def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_response_duration_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="quantile"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_response_duration_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_response_length | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="quantile"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_response_length_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_timeouts_total | counter">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="grpc_status"></def><def title="cause"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_total_duration_seconds | summary">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="quantile"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="grpc_service_total_duration_seconds_max | gauge">
        <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="hostname_pattern"></def><def title="method"></def><def title="service"></def><def title="grpc_status"></def><def title="http_status"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
</deflist>

## Processor metrics
<deflist type="full" sorted="asc">
      <def title="process_cpu_usage | gauge">
    The "recent cpu usage" for the Java Virtual Machine process
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="system_cpu_count | gauge">
    The number of processors available to the Java virtual machine
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="system_cpu_usage | gauge">
    The "recent cpu usage" of the system the application is running in
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="system_load_average_1m | gauge">
    The sum of the number of runnable entities queued to available processors and the number of runnable entities running on the available processors averaged over a period of time
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
</deflist>

## JVM metrics
<deflist type="full" sorted="asc">
      <def title="jvm_buffer_count_buffers | gauge">
    An estimate of the number of buffers in the pool
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_buffer_memory_used_bytes | gauge">
    An estimate of the memory that the Java virtual machine is using for this buffer pool
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_buffer_total_capacity_bytes | gauge">
    An estimate of the total capacity of the buffers in this pool
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_classes_loaded_classes | gauge">
    The number of classes that are currently loaded in the Java virtual machine
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_classes_unloaded_classes_total | counter">
    The total number of classes unloaded since the Java virtual machine has started execution
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_gc_concurrent_phase_time_seconds | summary">
    Time spent in concurrent phase
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="action"></def><def title="cause"></def><def title="gc"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_gc_concurrent_phase_time_seconds_max | gauge">
    Time spent in concurrent phase
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="action"></def><def title="cause"></def><def title="gc"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_gc_live_data_size_bytes | gauge">
    Size of long-lived heap memory pool after reclamation
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_gc_max_data_size_bytes | gauge">
    Max size of long-lived heap memory pool
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_gc_memory_allocated_bytes_total | counter">
    Incremented for an increase in the size of the (young) heap memory pool after one GC to before the next
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_gc_pause_seconds | summary">
    Time spent in GC pause
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="action"></def><def title="cause"></def><def title="gc"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_gc_pause_seconds_max | gauge">
    Time spent in GC pause
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="action"></def><def title="cause"></def><def title="gc"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_memory_committed_bytes | gauge">
    The amount of memory in bytes that is committed for the Java virtual machine to use
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="area"></def><def title="id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_memory_max_bytes | gauge">
    The maximum amount of memory in bytes that can be used for memory management
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="area"></def><def title="id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_memory_used_bytes | gauge">
    The amount of used memory
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="area"></def><def title="id"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_threads_daemon_threads | gauge">
    The current number of live daemon threads
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_threads_live_threads | gauge">
    The current number of live threads including both daemon and non-daemon threads
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_threads_peak_threads | gauge">
    The peak live thread count since the Java virtual machine started or peak was reset
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_threads_started_threads_total | counter">
    The total number of application threads started in the JVM
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
  <def title="jvm_threads_states_threads | gauge">
    The current number of threads
    <deflist type="full" collapsible="true">
      <def title="labels" default-state="collapsed">
        <deflist type="full">
          <def title="state"></def>
          <include from="Metrics-reference.md" element-id="common-configs" />
        </deflist>
      </def>
    </deflist>
  </def>
</deflist>