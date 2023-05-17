package com.slack.kaldb.metadata.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SearchMetadataStoreTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private MetadataStore zkMetadataStore;
  private SearchMetadataStore store;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
    CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
    zkMetadataStore =
        new ZookeeperMetadataStoreImpl(
            testingServer.getConnectString(),
            "test",
            1000,
            1000,
            new RetryNTimes(1, 500),
            countingFatalErrorHandler,
            meterRegistry);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (store != null) store.close();
    zkMetadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testSearchMetadataStoreIsNotUpdatable() throws Exception {
    store = new SearchMetadataStore(zkMetadataStore, true);
    SearchMetadata searchMetadata = new SearchMetadata("test", "snapshot", "http");
    Throwable ex = catchThrowable(() -> store.update(searchMetadata));
    assertThat(ex).isInstanceOf(UnsupportedOperationException.class);
  }
}
