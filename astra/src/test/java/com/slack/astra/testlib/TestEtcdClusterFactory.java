package com.slack.astra.testlib;

import com.google.protobuf.ByteString;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.launcher.Etcd;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.OptionsUtil;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory that manages a singleton etcd cluster for tests. This factory reuses a single embedded
 * etcd cluster across test classes to avoid the overhead of starting and stopping multiple
 * clusters.
 */
public class TestEtcdClusterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestEtcdClusterFactory.class);
  private static final String DEFAULT_CLUSTER_NAME = "test-etcd-cluster";
  private static final int DEFAULT_NODE_COUNT = 1;
  private static final long DELETE_TIMEOUT_MS = 5000;

  private static EtcdCluster etcdCluster;
  private static boolean initialized = false;
  private static int refCount = 0;

  /**
   * Start the etcd cluster if it's not already running. This method is synchronized to prevent
   * concurrent start/stop operations.
   *
   * @return The running etcd cluster.
   */
  public static synchronized EtcdCluster start() {
    refCount++;
    if (!initialized) {
      LOG.info("Starting embedded etcd cluster");
      etcdCluster =
          Etcd.builder()
              .withClusterName(DEFAULT_CLUSTER_NAME + "-" + UUID.randomUUID())
              .withNodes(DEFAULT_NODE_COUNT)
              .build();
      try {
        etcdCluster.start();
        initialized = true;
        LOG.info(
            "Embedded etcd cluster started with endpoints: {}",
            etcdCluster.clientEndpoints().stream().map(Object::toString).toList());
      } catch (Exception e) {
        LOG.error("Failed to start embedded etcd cluster", e);
        throw new RuntimeException("Failed to start embedded etcd cluster", e);
      }

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    LOG.info("Shutdown hook for embedded etcd cluster");
                    TestEtcdClusterFactory.close();
                  }));
    } else {
      LOG.info(
          "Reusing existing etcd cluster with endpoints: {}",
          etcdCluster.clientEndpoints().stream().map(Object::toString).toList());
    }
    TestEtcdClusterFactory.clearCluster();
    return etcdCluster;
  }

  /** Clear all data from the etcd cluster. This method will delete all keys in the cluster. */
  public static void clearCluster() {
    if (!initialized) {
      LOG.warn("Attempted to clear a cluster that is not initialized");
      return;
    }

    try (Client client = createClient()) {
      var end = OptionsUtil.prefixEndOf(ByteSequence.from(new byte[] {}));
      CompletableFuture<DeleteResponse> deleteFuture =
          client
              .getKVClient()
              .delete(
                  ByteSequence.from(ByteString.copyFromUtf8("*")),
                  DeleteOption.builder().isPrefix(true).withRange(end).build());

      try {
        deleteFuture.get(DELETE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.error("Failed to clear etcd cluster", e);
        throw new RuntimeException("Failed to clear etcd cluster", e);
      }
    }
  }

  /**
   * Close the etcd cluster. This method is synchronized to prevent concurrent start/stop
   * operations.
   */
  public static synchronized void close() {
    etcdCluster.close();
    initialized = false;
  }

  /**
   * Get the endpoints of the etcd cluster.
   *
   * @return A List of endpoint strings.
   */
  public static List<String> getEndpoints() {
    if (!initialized) {
      throw new IllegalStateException("Etcd cluster is not initialized");
    }
    return etcdCluster.clientEndpoints().stream().map(Object::toString).toList();
  }

  /**
   * Create a client connected to the etcd cluster.
   *
   * @return A new etcd client.
   */
  public static Client createClient() {
    if (!initialized) {
      throw new IllegalStateException("Etcd cluster is not initialized");
    }

    ClientBuilder clientBuilder =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    return clientBuilder.build();
  }

  /**
   * Check if the etcd cluster is currently running.
   *
   * @return True if the cluster is running, false otherwise.
   */
  public static boolean isRunning() {
    return initialized && etcdCluster != null;
  }
}
