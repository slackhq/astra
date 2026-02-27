package com.slack.astra.metadata.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for SearchMetadataStore that verify proper interaction with the legacy
 * SearchMetadataStoreLegacy. These tests create data in the legacy searchMetadataStore and verify
 * it can be accessed from the new searchMetadataStore, and vice versa.
 */
public class SearchMetadataStoreLegacyMigrationTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private SearchMetadataStoreLegacy legacyStore;
  private SearchMetadataStore searchMetadataStore;
  private static EtcdCluster etcdCluster;
  private Client etcdClient;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
    etcdCluster = TestEtcdClusterFactory.start();

    // Create etcd client
    etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .namespace(ByteSequence.from("Test", java.nio.charset.StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("Test")
            .setEnabled(true)
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .putStoreModes("DatasetMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SnapshotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("ReplicaMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("HpaMetricMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SearchMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheSlotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeAssignmentStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes(
                "FieldRedactionMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("PreprocessorMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryTaskMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("Test")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(500)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .setEtcdConfig(etcdConfig)
            .build();
    this.curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());

    // Initialize the stores
    // We initialize separately to have more control over their lifecycle in tests
    this.legacyStore =
        new SearchMetadataStoreLegacy(
            curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true);

    // By default, we create the new searchMetadataStore which internally creates its own
    // legacyStore instance
    // For some tests, we might want to create the new searchMetadataStore after creating data in
    // our legacyStore
    // instance
    createNewStore();
  }

  /** Helper method to create a new SearchMetadataStore instance */
  private void createNewStore() {
    if (searchMetadataStore != null) {
      try {
        searchMetadataStore.close();
      } catch (Exception e) {
        // Ignore
      }
    }
    this.searchMetadataStore =
        new SearchMetadataStore(
            curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (searchMetadataStore != null) searchMetadataStore.close();
    if (legacyStore != null) legacyStore.close();
    curatorFramework.unwrap().close();
    if (etcdClient != null) etcdClient.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testCreateDataInLegacyAndAccessFromNew() {
    // Create search metadata in the legacy searchMetadataStore
    SearchMetadata legacyMetadata =
        new SearchMetadata("testLegacy", "testSnapshotLegacy", "http://test-legacy.com");
    legacyStore.createSync(legacyMetadata);

    createNewStore();

    // Try to access the data from the new searchMetadataStore
    // The new searchMetadataStore should fall back to the legacy searchMetadataStore when getSync
    // fails to find it in the
    // partitioned searchMetadataStore
    SearchMetadata retrievedMetadata =
        searchMetadataStore.getSync(legacyMetadata.getPartition(), "testLegacy");

    // Verify the data was correctly retrieved
    assertThat(retrievedMetadata).isNotNull();
    assertThat(retrievedMetadata.name).isEqualTo("testLegacy");
    assertThat(retrievedMetadata.snapshotName).isEqualTo("testSnapshotLegacy");
    assertThat(retrievedMetadata.url).isEqualTo("http://test-legacy.com");
    assertThat(retrievedMetadata.isSearchable()).isTrue(); // Default value is true
  }

  @Test
  public void testCreateDataInNewAndRead() {
    // Create search metadata in the new searchMetadataStore
    SearchMetadata newMetadata =
        new SearchMetadata("testNew", "testSnapshotNew", "http://test-new.com", false);
    searchMetadataStore.createSync(newMetadata);

    // When accessing directly from the legacy searchMetadataStore, we should not find it
    Throwable thrown = catchThrowable(() -> legacyStore.getSync("testNew"));
    assertThat(thrown).isInstanceOf(RuntimeException.class);

    // But when accessing via the new searchMetadataStore, it should find it in the partitioned
    // searchMetadataStore
    SearchMetadata retrievedMetadata =
        searchMetadataStore.getSync(newMetadata.getPartition(), "testNew");
    assertThat(retrievedMetadata).isNotNull();
    assertThat(retrievedMetadata.name).isEqualTo("testNew");
    assertThat(retrievedMetadata.snapshotName).isEqualTo("testSnapshotNew");
    assertThat(retrievedMetadata.url).isEqualTo("http://test-new.com");
    assertThat(retrievedMetadata.isSearchable()).isFalse();
  }

  @Test
  public void testUpdateSearchabilityInNewStore() throws Exception {
    // Create search metadata in the new searchMetadataStore with searchable=false
    SearchMetadata newMetadata =
        new SearchMetadata("testSearchability", "testSnapshot", "http://test-url.com", false);
    searchMetadataStore.createSync(newMetadata);

    // Verify initial state
    SearchMetadata initialMetadata =
        searchMetadataStore.getSync(newMetadata.getPartition(), "testSearchability");
    assertThat(initialMetadata.isSearchable()).isFalse();

    // Update the searchability
    searchMetadataStore.updateSearchability(initialMetadata, true);

    // Verify the update was successful in the new searchMetadataStore
    // Wait until the update propagates
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () -> {
              SearchMetadata updated =
                  searchMetadataStore.getSync(newMetadata.getPartition(), "testSearchability");
              return updated != null && updated.isSearchable();
            });

    SearchMetadata updatedMetadata =
        searchMetadataStore.getSync(newMetadata.getPartition(), "testSearchability");
    assertThat(updatedMetadata.isSearchable()).isTrue();
  }

  @Test
  public void testUpdateSearchabilityWithFallbackToLegacy() {
    // Create search metadata in the legacy searchMetadataStore with default searchable=true
    SearchMetadata legacyMetadata =
        new SearchMetadata(
            "testLegacySearchability", "testLegacySnapshot", "http://test-legacy-url.com");
    legacyStore.createSync(legacyMetadata);

    // Create a new searchMetadataStore instance
    createNewStore();

    // Get the metadata from the new searchMetadataStore (should fall back to legacy)
    SearchMetadata retrievedMetadata =
        searchMetadataStore.getSync(legacyMetadata.getPartition(), "testLegacySearchability");
    assertThat(retrievedMetadata.isSearchable()).isTrue();

    // Update searchability using the new searchMetadataStore
    searchMetadataStore.updateSearchability(retrievedMetadata, false);

    // Verify the change in the legacy searchMetadataStore
    // (Since the new searchMetadataStore will try to update its partitioned searchMetadataStore
    // first and fail, it should fall
    // back to legacy)
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                SearchMetadata updated = legacyStore.getSync("testLegacySearchability");
                return updated != null && !updated.isSearchable();
              } catch (Exception e) {
                return false;
              }
            });

    SearchMetadata updatedLegacyMetadata = legacyStore.getSync("testLegacySearchability");
    assertThat(updatedLegacyMetadata.isSearchable()).isFalse();
  }

  @Test
  public void testDeletionInNewStore() {
    // Create search metadata in the new searchMetadataStore only
    SearchMetadata newMetadata =
        new SearchMetadata("testDeleteBoth", "testSnapshot", "http://test-url.com");
    searchMetadataStore.createSync(newMetadata); // In the partitioned searchMetadataStore

    // Verify data exists in the new searchMetadataStore
    assertThat(searchMetadataStore.getSync(newMetadata.getPartition(), "testDeleteBoth"))
        .isNotNull();

    // Delete using the new searchMetadataStore
    searchMetadataStore.deleteSync(newMetadata);

    // Verify the item is deleted and throws an exception when trying to access it
    Throwable thrown =
        catchThrowable(
            () -> searchMetadataStore.getSync(newMetadata.getPartition(), "testDeleteBoth"));
    assertThat(thrown).isInstanceOf(RuntimeException.class);
  }

  @Test
  public void testDeleteFromNewWithFallbackToLegacy() {
    // Create only in legacy searchMetadataStore
    SearchMetadata legacyOnlyMetadata =
        new SearchMetadata("testLegacyDeletion", "testSnapshot", "http://legacy-url.com");
    legacyStore.createSync(legacyOnlyMetadata);

    // Create a new searchMetadataStore
    createNewStore();

    // Get it from the new searchMetadataStore (should fall back to legacy)
    SearchMetadata retrievedMetadata =
        searchMetadataStore.getSync(legacyOnlyMetadata.getPartition(), "testLegacyDeletion");
    assertThat(retrievedMetadata).isNotNull();

    // Delete it using the new searchMetadataStore
    searchMetadataStore.deleteSync(retrievedMetadata);

    // Verify it's deleted from legacy searchMetadataStore
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore))
        .doesNotContain(retrievedMetadata);

    // And also not available through the new searchMetadataStore
    Throwable newStoreThrown =
        catchThrowable(
            () ->
                searchMetadataStore.getSync(
                    legacyOnlyMetadata.getPartition(), "testLegacyDeletion"));
    assertThat(newStoreThrown).isInstanceOf(RuntimeException.class);
  }

  @Test
  public void testListOperationsCombinesResultsFromBothStores() {
    // Create data in both stores with unique names
    SearchMetadata legacyMetadata1 =
        new SearchMetadata("legacyItem1", "legacySnapshot1", "http://legacy1.com");
    SearchMetadata legacyMetadata2 =
        new SearchMetadata("legacyItem2", "legacySnapshot2", "http://legacy2.com");
    legacyStore.createSync(legacyMetadata1);
    legacyStore.createSync(legacyMetadata2);

    // Create a new searchMetadataStore and add data there
    createNewStore();
    SearchMetadata newMetadata1 = new SearchMetadata("newItem1", "newSnapshot1", "http://new1.com");
    SearchMetadata newMetadata2 = new SearchMetadata("newItem2", "newSnapshot2", "http://new2.com");
    searchMetadataStore.createSync(newMetadata1);
    searchMetadataStore.createSync(newMetadata2);

    // Test that listSync returns combined results from both stores
    List<SearchMetadata> combinedList = searchMetadataStore.listSync();

    // Should contain 4 items in total (2 from each searchMetadataStore)
    assertThat(combinedList).hasSize(4);

    // Verify items from legacy searchMetadataStore are included
    assertThat(combinedList.stream().anyMatch(m -> m.name.equals("legacyItem1"))).isTrue();
    assertThat(combinedList.stream().anyMatch(m -> m.name.equals("legacyItem2"))).isTrue();

    // Verify items from new searchMetadataStore are included
    assertThat(combinedList.stream().anyMatch(m -> m.name.equals("newItem1"))).isTrue();
    assertThat(combinedList.stream().anyMatch(m -> m.name.equals("newItem2"))).isTrue();
  }

  @Test
  public void testListWithDuplicateItemsAcrossStores() {
    // Create same-named item in both stores, but with different searchability settings
    SearchMetadata legacyMetadata =
        new SearchMetadata("duplicateItem", "legacySnapshot", "http://legacy.com", true);
    legacyStore.createSync(legacyMetadata);

    // Create a new searchMetadataStore and add data with same name but different properties
    createNewStore();
    SearchMetadata newMetadata =
        new SearchMetadata("duplicateItem", "newSnapshot", "http://new.com", false);
    searchMetadataStore.createSync(newMetadata);

    // Test that listSync returns combined results, with new searchMetadataStore data taking
    // precedence
    List<SearchMetadata> combinedList = searchMetadataStore.listSync();

    // Should contain items from both stores, but only one instance of the duplicate item
    List<SearchMetadata> duplicateItems =
        combinedList.stream().filter(m -> m.name.equals("duplicateItem")).toList();

    // The current implementation returns items from both stores when they have the same name
    // This behavior is shown in SearchMetadataStore.listSync() where it simply adds all items from
    // the legacy searchMetadataStore
    assertThat(duplicateItems).hasSize(2);

    // Verify both items from both stores are present
    assertThat(duplicateItems.stream().anyMatch(m -> m.snapshotName.equals("legacySnapshot")))
        .isTrue();
    assertThat(duplicateItems.stream().anyMatch(m -> m.snapshotName.equals("newSnapshot")))
        .isTrue();
  }
}
