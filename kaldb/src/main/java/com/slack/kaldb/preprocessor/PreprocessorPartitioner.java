package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_STAR_SERVICE;
import static com.slack.kaldb.preprocessor.PreprocessorService.sortDatasetsOnThroughput;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.service.murron.trace.Trace;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class PreprocessorPartitioner<K, V> implements StreamPartitioner<String, Trace.Span> {

  final List<DatasetMetadata> throughputSortedDatasets;
  ConcurrentHashMap<String, Supplier<Integer>> datasetPartitionSuppliers;

  public PreprocessorPartitioner(
      List<DatasetMetadata> datasetMetadataList, int kafkaPartitionStickyTimeoutMs) {
    this.throughputSortedDatasets = sortDatasetsOnThroughput(datasetMetadataList);
    this.datasetPartitionSuppliers =
        getDatasetPartitionSuppliers(datasetMetadataList, kafkaPartitionStickyTimeoutMs);
  }

  /**
   * When kafka streams produces data to write to the preprocessor, instead of choosing partitions
   * at random for each dataset we want to route all documents to a single partition for a set
   * time(stickyTimeoutMs). We pick the partition at random and doesn't have smart routing like
   * picking partitions which have lesser load etc. The motivation here is we create better batches
   * while producing data into kafka and thereby improving the efficiency We use Guava's
   * Suppliers.memoizeWithExpiration library which gives us a nice construct to achieve this
   */
  private ConcurrentHashMap<String, Supplier<Integer>> getDatasetPartitionSuppliers(
      List<DatasetMetadata> datasetMetadataList, int kafkaPartitionStickyTimeoutMs) {
    ConcurrentHashMap<String, Supplier<Integer>> datasetPartitionSuppliers =
        new ConcurrentHashMap<>();
    for (DatasetMetadata datasetMetadata : datasetMetadataList) {
      if (kafkaPartitionStickyTimeoutMs > 0) {
        datasetPartitionSuppliers.put(
            datasetMetadata.name,
            Suppliers.memoizeWithExpiration(
                () -> {
                  List<Integer> partitions =
                      PreprocessorService.getActivePartitionList(datasetMetadata);
                  return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
                },
                kafkaPartitionStickyTimeoutMs,
                TimeUnit.MILLISECONDS));
      } else {
        datasetPartitionSuppliers.put(datasetMetadata.name, new NonStickySupplier(datasetMetadata));
      }
    }
    return datasetPartitionSuppliers;
  }

  /**
   * Implements the Supplier interface to not cache partitions i.e if
   * kafkaPartitionStickyTimeoutMs=0 we will return a new partition at random on every invocation
   */
  protected static class NonStickySupplier implements Supplier<Integer> {

    private final DatasetMetadata datasetMetadata;

    NonStickySupplier(DatasetMetadata datasetMetadata) {
      this.datasetMetadata = datasetMetadata;
    }

    @Override
    public Integer get() {
      List<Integer> partitions = PreprocessorService.getActivePartitionList(datasetMetadata);
      return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
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
        return datasetPartitionSuppliers.get(datasetMetadata.name).get();
      }
    }
    // this shouldn't happen, as we should have filtered all the missing datasets in the value
    // mapper stage
    throw new IllegalStateException(
        String.format("Service name '%s' was not found in dataset metadata", serviceName));
  }
}
