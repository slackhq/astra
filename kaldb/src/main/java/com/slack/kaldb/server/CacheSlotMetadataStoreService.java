package com.slack.kaldb.server;

import static com.slack.kaldb.config.KaldbConfig.CACHE_SLOT_STORE_PATH;
import static com.slack.kaldb.metadata.cache.CacheSlotMetadata.METADATA_SLOT_NAME;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.chunk.ChunkManager;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheSlotMetadataStoreService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(CacheSlotMetadataStoreService.class);

  private final MetadataStoreService metadataStoreService;
  private final KaldbConfigs.CacheConfig cacheConfig;
  private final ChunkManager<LogMessage> chunkManager;
  public Map<String, CacheSlotMetadataStore> cacheSlotMap = new ConcurrentHashMap<>();

  // by using a single thread executor we can ensure that the hydration from blob storage is done
  // serially since we expect network speed to be the primary constraint when loading new snapshots,
  // this should allow us to make snapshot available for query sooner than parallel loading would
  private final ExecutorService executorService =
      Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder().setNameFormat("cache-slot--%d").build());

  public CacheSlotMetadataStoreService(
      MetadataStoreService metadataStoreService,
      ChunkManager<LogMessage> chunkManager,
      KaldbConfigs.CacheConfig cacheConfig) {
    this.metadataStoreService = metadataStoreService;
    this.chunkManager = chunkManager;
    this.cacheConfig = cacheConfig;
  }

  @Override
  protected void startUp() throws Exception {
    metadataStoreService.awaitRunning(KaldbConfig.DEFAULT_START_STOP_DURATION);
    String serverAddress = cacheConfig.getServerConfig().getServerAddress();

    IntStream.rangeClosed(0, Math.toIntExact(cacheConfig.getSlotsPerInstance() - 1))
        .forEach(
            (slotIterator) -> {
              String slotId = String.format("%s-%s", serverAddress, slotIterator);
              String cacheSlotPath = String.format("%s/%s", CACHE_SLOT_STORE_PATH, slotId);
              initializeSlot(slotId, cacheSlotPath);
            });
  }

  @Override
  protected void shutDown() throws Exception {
    // we can reasonably force shutdown the executor service, as nothing in progress would be
    // important to complete
    executorService.shutdownNow();
    cacheSlotMap.forEach((slotId, cacheSlotMetadataStore) -> cacheSlotMetadataStore.close());
  }

  private void initializeSlot(String slotId, String cacheSlotPath) {
    try {
      // todo - we should consider refactoring the constructor to allow setting the node data
      // this would allow us to remove the need for the /SLOT node
      // /cacheSlot/10.11.12.13-0/SLOT

      CacheSlotMetadataStore cacheSlotMetadataStore =
          new CacheSlotMetadataStore(metadataStoreService.getMetadataStore(), cacheSlotPath);
      cacheSlotMetadataStore.addListener(cacheNodeListener(slotId));
      cacheSlotMap.put(slotId, cacheSlotMetadataStore);
      registerSlotOnMetadataStore(slotId);
    } catch (Exception e) {
      LOG.error("Error instantiating cache slot", e);
    }
  }

  private KaldbMetadataStoreChangeListener cacheNodeListener(String slotId) {
    return () -> {
      CacheSlotMetadata cacheSlotMetadata = cacheSlotMap.get(slotId).getCached().get(0);
      LOG.debug(String.format("Change on slot '%s' - %s", slotId, cacheSlotMetadata.toString()));

      if (cacheSlotMetadata.cacheSlotState.equals(Metadata.CacheSlotState.ASSIGNED)) {
        LOG.info(String.format("Slot %s - ASSIGNED received", slotId));
        executorService.submit(() -> handleSlotAssignment(slotId));
      } else if (cacheSlotMetadata.cacheSlotState.equals(Metadata.CacheSlotState.EVICT)) {
        LOG.info(String.format("Slot %s - EVICT received", slotId));
        executorService.submit(() -> handleSlotEviction(slotId));
      }
    };
  }

  private void registerSlotOnMetadataStore(String slotId)
      throws ExecutionException, InterruptedException, TimeoutException {
    cacheSlotMap
        .get(slotId)
        .create(
            new CacheSlotMetadata(
                METADATA_SLOT_NAME, Metadata.CacheSlotState.FREE, "", Instant.now().toEpochMilli()))
        .get(2, TimeUnit.SECONDS);
  }

  private void handleSlotAssignment(String slotId) {
    CacheSlotMetadata cacheSlotMetadata = cacheSlotMap.get(slotId).getCached().get(0);

    // todo - load asset from S3 into chunkManager
  }

  private void handleSlotEviction(String slotId) {
    CacheSlotMetadata cacheSlotMetadata = cacheSlotMap.get(slotId).getCached().get(0);

    // todo - evict asset from chunkManager
  }
}
