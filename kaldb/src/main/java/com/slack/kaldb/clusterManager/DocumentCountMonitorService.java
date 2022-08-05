package com.slack.kaldb.clusterManager;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.kaldb.logstore.search.KaldbDistributedQueryService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DocumentCountMonitorService extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(DocumentCountMonitorService.class);

  KaldbDistributedQueryService kaldbDistributedQueryService;
  KaldbConfigs.ManagerConfig managerConfig;
  MeterRegistry meterRegistry;
  AtomicInteger hitCount;

  public DocumentCountMonitorService(
      KaldbDistributedQueryService kaldbDistributedQueryService,
      KaldbConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {
    this.kaldbDistributedQueryService = kaldbDistributedQueryService;
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;

    this.meterRegistry.gauge(
            "cached_service_nodes_size", hitCount.intValue());

  }

  @Override
  protected AbstractScheduledService.Scheduler scheduler() {
    return AbstractScheduledService.Scheduler.newFixedRateSchedule(
            managerConfig.getScheduleInitialDelayMins(),
            managerConfig.getReplicaAssignmentServiceConfig().getSchedulePeriodMins(),
            TimeUnit.MINUTES);
  }

  @Override
  protected void runOneIteration() throws Exception {
    KaldbSearch.SearchRequest searchRequest = KaldbSearch.SearchRequest.newBuilder()
            .setDataset("_all")
            .setQueryString("*:*")
            .setStartTimeEpochMs(0)
            .setEndTimeEpochMs(1)
            .setHowMany(100)
            .setBucketCount(2)
            .build();

    KaldbSearch.SearchResult searchResult = kaldbDistributedQueryService.doSearch(searchRequest);
    hitCount.set(searchResult.getHitsCount());
  }

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
  }
}
