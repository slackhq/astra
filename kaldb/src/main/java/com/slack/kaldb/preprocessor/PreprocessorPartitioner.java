package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_STAR_SERVICE;
import static com.slack.kaldb.preprocessor.PreprocessorService.sortDatasetsOnThroughput;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.service.murron.trace.Trace;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class PreprocessorPartitioner<K, V> implements StreamPartitioner<String, Trace.Span> {

  final List<DatasetMetadata> throughputSortedDatasets;
  final int stickyTimeoutMs;
  ConcurrentHashMap<String, Supplier<Integer>> topicPartitionSuppliers = new ConcurrentHashMap<>();

  public PreprocessorPartitioner(List<DatasetMetadata> datasetMetadataList, int stickyTimeoutMs) {
    this.throughputSortedDatasets = sortDatasetsOnThroughput(datasetMetadataList);
    this.stickyTimeoutMs = stickyTimeoutMs;
    for (DatasetMetadata datasetMetadata : datasetMetadataList) {
      topicPartitionSuppliers.put(
          datasetMetadata.name,
          Suppliers.memoizeWithExpiration(
              () -> {
                List<Integer> partitions =
                    PreprocessorService.getActivePartitionList(datasetMetadata);
                return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
              },
              stickyTimeoutMs,
              TimeUnit.MILLISECONDS));
    }
  }

  /**
   * Returns a partition from the provided list of dataset metadata. If no valid dataset metadata
   * are provided throws an exception. The partition is cached for a set period ( stickyTimeoutMs )
   */
  @Override
  public Integer partition(
      final String topic, final String key, final Trace.Span value, final int numPartitions) {
    String serviceName = PreprocessorValueMapper.getServiceName(value);
    if (serviceName == null) {
      // this also should not happen since we drop messages with empty service names in the rate
      // limiter
      throw new IllegalStateException(
          String.format("Service name not found within the message '%s'", value));
    }

    for (DatasetMetadata datasetMetadata : throughputSortedDatasets) {
      String serviceNamePattern = datasetMetadata.getServiceNamePattern();
      // back-compat since this is a new field
      if (serviceNamePattern == null) {
        serviceNamePattern = datasetMetadata.getName();
      }

      if (serviceNamePattern.equals(MATCH_ALL_SERVICE)
          || serviceNamePattern.equals(MATCH_STAR_SERVICE)
          || serviceName.equals(serviceNamePattern)) {
        return topicPartitionSuppliers.get(datasetMetadata.name).get();
      }
    }
    // this shouldn't happen, as we should have filtered all the missing datasets in the value
    // mapper stage
    throw new IllegalStateException(
        String.format("Service name '%s' was not found in dataset metadata", serviceName));
  }
}
