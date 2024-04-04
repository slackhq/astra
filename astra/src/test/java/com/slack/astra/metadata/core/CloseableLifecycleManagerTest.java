package com.slack.astra.metadata.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.slack.astra.proto.config.AstraConfigs;
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
        new CloseableLifecycleManager(AstraConfigs.NodeRole.INDEX, closableList);
    closeableLifecycleManager.shutDown();

    assertEquals(1, successful.get());
    assertEquals(1, failed.get());
  }
}
