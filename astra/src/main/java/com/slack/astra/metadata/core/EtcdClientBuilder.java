package com.slack.astra.metadata.core;

import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import com.google.common.base.Strings;
import com.slack.astra.proto.config.AstraConfigs.EtcdConfig;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builder class to instantiate a common etcd client for use in the metadata stores. */
public class EtcdClientBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(EtcdClientBuilder.class);

  /**
   * Builds and returns an etcd client based on the provided configuration.
   *
   * @param etcdConfig The etcd configuration
   * @return A configured etcd client
   */
  public static Client build(EtcdConfig etcdConfig) {
    ensureTrue(etcdConfig.getEnabled(), "Etcd must be enabled to build a client");
    ensureTrue(
        !etcdConfig.getEndpointsList().isEmpty(), "At least one etcd endpoint must be provided");

    ClientBuilder clientBuilder = Client.builder();

    // Configure endpoints
    clientBuilder.endpoints(etcdConfig.getEndpointsList().toArray(new String[0]));

    // Configure timeouts
    if (etcdConfig.getConnectionTimeoutMs() > 0) {
      clientBuilder.connectTimeout(Duration.ofMillis(etcdConfig.getConnectionTimeoutMs()));
    }

    if (etcdConfig.getKeepaliveTimeoutMs() > 0) {
      clientBuilder.keepaliveTimeout(Duration.ofMillis(etcdConfig.getKeepaliveTimeoutMs()));
    }

    // Configure retries
    if (etcdConfig.getOperationsMaxRetries() > 0) {
      clientBuilder.retryMaxAttempts(etcdConfig.getOperationsMaxRetries());
    }

    if (etcdConfig.getRetryDelayMs() > 0) {
      clientBuilder.retryDelay(etcdConfig.getRetryDelayMs());
    }

    // Set namespace if provided
    if (!Strings.isNullOrEmpty(etcdConfig.getNamespace())) {
      clientBuilder.namespace(ByteSequence.from(etcdConfig.getNamespace(), StandardCharsets.UTF_8));
    }

    Client client = clientBuilder.build();

    LOG.info(
        "Started etcd client with the following config: endpoints: {}, namespace: {}, "
            + "connection timeout ms: {}, keepalive timeout ms: {}, max retries: {}, retry delay ms: {}",
        String.join(",", etcdConfig.getEndpointsList()),
        etcdConfig.getNamespace(),
        etcdConfig.getConnectionTimeoutMs(),
        etcdConfig.getKeepaliveTimeoutMs(),
        etcdConfig.getOperationsMaxRetries(),
        etcdConfig.getRetryDelayMs());

    return client;
  }
}
