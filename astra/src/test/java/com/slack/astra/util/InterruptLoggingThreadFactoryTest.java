package com.slack.astra.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

public class InterruptLoggingThreadFactoryTest {

  /**
   * The factory's threads must still behave as normal interruptible threads: calling interrupt()
   * sets the flag (after logging the caller stack) so the diagnostic never changes runtime
   * behavior. We assert the running thread observes its own interrupt.
   */
  @Test
  public void interruptStillSetsTheFlagAfterLogging() throws InterruptedException {
    InterruptLoggingThreadFactory factory =
        new InterruptLoggingThreadFactory("test-interrupt-logging-%d");

    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(1);
    AtomicBoolean sawInterrupt = new AtomicBoolean(false);

    Thread thread =
        factory.newThread(
            () -> {
              started.countDown();
              try {
                // Block until interrupted.
                Thread.sleep(TimeUnit.SECONDS.toMillis(30));
              } catch (InterruptedException e) {
                sawInterrupt.set(true);
              } finally {
                done.countDown();
              }
            });

    thread.start();
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

    thread.interrupt();

    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(sawInterrupt.get()).isTrue();
  }

  @Test
  public void appliesTheConfiguredNameFormat() {
    InterruptLoggingThreadFactory factory = new InterruptLoggingThreadFactory("named-worker-%d");
    Thread thread = factory.newThread(() -> {});
    assertThat(thread.getName()).isEqualTo("named-worker-0");
  }
}
