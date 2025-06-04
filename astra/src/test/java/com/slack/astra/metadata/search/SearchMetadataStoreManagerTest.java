package com.slack.astra.metadata.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SearchMetadataStoreManagerTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.ZookeeperConfig zkConfig;
  private SearchMetadataStoreManager storeManager;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("Test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .setZkCacheInitTimeoutMs(1000)
            .build();
    this.curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (storeManager != null) storeManager.close();
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testPreferredStoreType() throws Exception {
    // Create with legacy preferred store type
    storeManager =
        new SearchMetadataStoreManager(
            curatorFramework, zkConfig, meterRegistry, true, SearchMetadataStoreType.LEGACY);

    // Create metadata only in partitioned store via direct access
    SearchMetadata partitionedMetadata =
        new SearchMetadata("partitioned-only", "snapshot-p", "http://example.com");
    storeManager.partitionedStore.createSync(partitionedMetadata);

    // Should still be able to get it with legacy as preferred type
    SearchMetadata retrievedViaLegacy = storeManager.getSync("partitioned-only");
    assertThat(retrievedViaLegacy).isNotNull();
    assertThat(retrievedViaLegacy.name).isEqualTo("partitioned-only");

    // Close the current store manager
    storeManager.close();

    // Create with partitioned preferred store type
    storeManager =
        new SearchMetadataStoreManager(
            curatorFramework, zkConfig, meterRegistry, true, SearchMetadataStoreType.PARTITIONED);

    // Create metadata only in legacy store via direct access
    SearchMetadata legacyMetadata =
        new SearchMetadata("legacy-only", "snapshot-l", "http://example.com");
    storeManager.legacyStore.createSync(legacyMetadata);

    // Should still be able to get it with partitioned as preferred type
    SearchMetadata retrievedViaPartitioned = storeManager.getSync("legacy-only");
    assertThat(retrievedViaPartitioned).isNotNull();
    assertThat(retrievedViaPartitioned.name).isEqualTo("legacy-only");
  }

  @Test
  public void testCreateAndGetMetadata() throws Exception {
    storeManager =
        new SearchMetadataStoreManager(
            curatorFramework, zkConfig, meterRegistry, true, SearchMetadataStoreType.LEGACY);
    SearchMetadata searchMetadata = new SearchMetadata("test1", "snapshot1", "http://example.com");
    storeManager.createSync(searchMetadata);

    // Should be able to get from storeManager
    SearchMetadata retrievedMetadata = storeManager.getSync("test1");
    assertThat(retrievedMetadata).isNotNull();
    assertThat(retrievedMetadata.name).isEqualTo("test1");
    assertThat(retrievedMetadata.snapshotName).isEqualTo("snapshot1");
    assertThat(retrievedMetadata.url).isEqualTo("http://example.com");
    assertThat(retrievedMetadata.isSearchable()).isTrue();
  }

  @Test
  public void testUpdateSearchability() throws Exception {
    storeManager =
        new SearchMetadataStoreManager(
            curatorFramework, zkConfig, meterRegistry, true, SearchMetadataStoreType.LEGACY);
    SearchMetadata searchMetadata =
        new SearchMetadata("test2", "snapshot2", "http://example.com", true);
    storeManager.createSync(searchMetadata);

    // Update searchability
    storeManager.updateSearchability("test2", false);

    // Verify the update
    SearchMetadata retrievedMetadata = storeManager.getSync("test2");
    assertThat(retrievedMetadata.isSearchable()).isFalse();
  }

  @Test
  public void testListMetadata() throws Exception {
    storeManager =
        new SearchMetadataStoreManager(
            curatorFramework, zkConfig, meterRegistry, true, SearchMetadataStoreType.LEGACY);

    // Create multiple metadata entries
    SearchMetadata metadata1 = new SearchMetadata("test3", "snapshot3", "http://example.com");
    SearchMetadata metadata2 = new SearchMetadata("test4", "snapshot4", "http://example.com");

    storeManager.createSync(metadata1);
    storeManager.createSync(metadata2);

    // List should return both entries
    List<SearchMetadata> metadataList = storeManager.listSync();
    assertThat(metadataList).hasSize(2);

    // Verify entries are correct
    assertThat(metadataList.stream().map(m -> m.name)).containsExactlyInAnyOrder("test3", "test4");
  }

  @Test
  public void testDeleteMetadata() throws Exception {
    storeManager =
        new SearchMetadataStoreManager(
            curatorFramework, zkConfig, meterRegistry, true, SearchMetadataStoreType.LEGACY);

    // Create metadata entry
    SearchMetadata metadata = new SearchMetadata("test5", "snapshot5", "http://example.com");
    storeManager.createSync(metadata);

    // Verify it exists
    SearchMetadata retrievedMetadata = storeManager.getSync("test5");
    assertThat(retrievedMetadata).isNotNull();

    // Delete the entry
    storeManager.deleteSync("test5");

    // Verify it's deleted by checking the list
    List<SearchMetadata> metadataList = storeManager.listSync();
    assertThat(metadataList).isEmpty();
  }

  @Test
  public void testChangeListener() throws Exception {
    storeManager =
        new SearchMetadataStoreManager(
            curatorFramework, zkConfig, meterRegistry, true, SearchMetadataStoreType.LEGACY);

    AtomicBoolean listenerCalled = new AtomicBoolean(false);

    // Add a listener
    storeManager.addListener(
        metadata -> {
          if (metadata.name.equals("test6")) {
            listenerCalled.set(true);
          }
        });

    // Create metadata entry
    SearchMetadata metadata = new SearchMetadata("test6", "snapshot6", "http://example.com");
    storeManager.createSync(metadata);

    // Verify listener was called
    await().atMost(5, TimeUnit.SECONDS).until(listenerCalled::get);
  }
}
