package com.slack.kaldb.metadata.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CloseableLifecycleManagerTest {

  @Test
  void testWhenOneClosableThrowsException() throws Exception {
    List<Closeable> closableList = new ArrayList<>();

    AtomicInteger failed = new AtomicInteger(0);
    closableList.add(
        () -> {
          failed.incrementAndGet();
          throw new IOException();
        });

    AtomicInteger successful = new AtomicInteger(0);
    closableList.add(successful::incrementAndGet);

    CloseableLifecycleManager closeableLifecycleManager =
        new CloseableLifecycleManager(KaldbConfigs.NodeRole.INDEX, closableList);
    closeableLifecycleManager.shutDown();

    assertEquals(1, successful.get());
    assertEquals(1, failed.get());
  }
}
