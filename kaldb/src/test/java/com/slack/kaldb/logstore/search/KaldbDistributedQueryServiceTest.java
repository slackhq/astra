package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.chunk.ChunkInfo.toSnapshotMetadata;
import static com.slack.kaldb.chunk.ReadWriteChunk.LIVE_SNAPSHOT_PREFIX;
import static com.slack.kaldb.chunk.ReadWriteChunk.toSearchMetadata;
import static com.slack.kaldb.logstore.search.KaldbDistributedQueryService.getSnapshotsToSearch;
import static com.slack.kaldb.metadata.snapshot.SnapshotMetadata.LIVE_SNAPSHOT_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.kaldb.chunk.ChunkInfo;
import com.slack.kaldb.chunk.ReadOnlyChunkImpl;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Collection;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KaldbDistributedQueryServiceTest {

  private SimpleMeterRegistry metricsRegistry;

  private MetadataStore zkMetadataStore;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private TestingServer testZKServer;
  private SearchContext indexerSearchContext;
  private SearchContext cache1SearchContext;
  private SearchContext cache2SearchContext;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();

    metricsRegistry = new SimpleMeterRegistry();
    testZKServer = new TestingServer();

    // Metadata store
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testZKServer.getConnectString())
            .setZkPathPrefix("indexerTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    zkMetadataStore = spy(ZookeeperMetadataStoreImpl.fromConfig(metricsRegistry, zkConfig));

    snapshotMetadataStore = spy(new SnapshotMetadataStore(zkMetadataStore, true));
    searchMetadataStore = spy(new SearchMetadataStore(zkMetadataStore, true));
    indexerSearchContext = new SearchContext("host1", 10000);
    cache1SearchContext = new SearchContext("host2", 20000);
    cache2SearchContext = new SearchContext("host3", 20001);
  }

  @After
  public void tearDown() throws Exception {
    snapshotMetadataStore.close();
    searchMetadataStore.close();
    zkMetadataStore.close();
    metricsRegistry.close();
    testZKServer.close();
  }

  @Test
  public void testOneIndexer() {
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    createIndexerZKMetadata(chunkCreationTime, chunkEndTime);

    Collection<String> snapshots =
        getSnapshotsToSearch(
            snapshotMetadataStore,
            searchMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli());
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(snapshots.iterator().next()).isEqualTo(indexerSearchContext.toString());

    snapshots =
        getSnapshotsToSearch(
            snapshotMetadataStore,
            searchMetadataStore,
            chunkEndTime.toEpochMilli() + 1,
            chunkEndTime.toEpochMilli() + 100);
    assertThat(snapshots.size()).isEqualTo(0);
  }

  @Test
  public void testOneIndexerOneCache() throws Exception {
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    createIndexerZKMetadata(chunkCreationTime, chunkEndTime);

    Collection<String> snapshots =
        getSnapshotsToSearch(
            snapshotMetadataStore,
            searchMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli());
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(snapshots.iterator().next()).isEqualTo(indexerSearchContext.toString());

    snapshots =
        getSnapshotsToSearch(
            snapshotMetadataStore,
            searchMetadataStore,
            chunkEndTime.toEpochMilli() + 1,
            chunkEndTime.toEpochMilli() + 100);
    assertThat(snapshots.size()).isEqualTo(0);

    // create cache node entry for search metadata also serving the snapshot
    String snapshotName = snapshotMetadataStore.getCached().iterator().next().name;
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache1SearchContext, snapshotName);
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(2);

    snapshots =
        getSnapshotsToSearch(
            snapshotMetadataStore, searchMetadataStore, 0, chunkCreationTime.toEpochMilli());
    assertThat(snapshots.size()).isEqualTo(1);
  }

  @Test
  public void testTwoCacheNodes() throws Exception {
    // create snapshot
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    SnapshotMetadata snapshotMetadata = createSnapshot(chunkCreationTime, chunkEndTime, false);

    // create first search metadata hosted by cache1
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache1SearchContext, snapshotMetadata.name);
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);

    // assert search will always find cache1
    Collection<String> snapshots =
        getSnapshotsToSearch(
            snapshotMetadataStore, searchMetadataStore, 0, chunkCreationTime.toEpochMilli());
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(snapshots.iterator().next()).isEqualTo(cache1SearchContext.toString());

    // create second search metadata hosted by cache2
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache2SearchContext, snapshotMetadata.name);
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(2);

    // assert search will always find cache1 or cache2
    snapshots =
        getSnapshotsToSearch(
            snapshotMetadataStore, searchMetadataStore, 0, chunkCreationTime.toEpochMilli());
    assertThat(snapshots.size()).isEqualTo(1);
  }

  @Test
  public void testNoNode() {
    Collection<String> snapshots =
        getSnapshotsToSearch(snapshotMetadataStore, searchMetadataStore, 0, Long.MAX_VALUE);
    assertThat(snapshots.size()).isEqualTo(0);
  }

  private void createIndexerZKMetadata(Instant chunkCreationTime, Instant chunkEndTime) {
    SnapshotMetadata liveSnapshotMetadata = createSnapshot(chunkCreationTime, chunkEndTime, true);
    SearchMetadata liveSearchMetadata =
        toSearchMetadata(liveSnapshotMetadata.snapshotId, indexerSearchContext);

    searchMetadataStore.createSync(liveSearchMetadata);
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);
  }

  private SnapshotMetadata createSnapshot(
      Instant chunkCreationTime, Instant chunkEndTime, boolean isLive) {
    ChunkInfo chunkInfo =
        new ChunkInfo(
            "chunkPrefix" + "_" + chunkCreationTime.getEpochSecond() + "_" + "logStoreId",
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            1234,
            "1",
            isLive ? LIVE_SNAPSHOT_PATH : "cacheSnapshotPath");
    SnapshotMetadata snapshotMetadata =
        toSnapshotMetadata(chunkInfo, isLive ? LIVE_SNAPSHOT_PREFIX : "");

    snapshotMetadataStore.createSync(snapshotMetadata);
    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(1);

    return snapshotMetadata;
  }
}
