package com.slack.kaldb.util;

import static org.assertj.core.api.Assertions.fail;

import org.apache.curator.test.TestingServer;

/**
 * This class is responsible for creating a testing ZK server for testing purposes. We create the ZK
 * server in a separate thread to avoid blocking the main thread. This improves the reliability of
 * the tests i.e I can put a debug point while running a test and ZKServer won't get blocked.
 * ZkServer getting blocked leads to a session expiry which will cause curator(the client) to
 * disconnect and call the runtime halter
 */
public class TestingZKServer {

  public static TestingServer createTestingServer() throws InterruptedException {
    ZKTestingServer zkTestingServer = new ZKTestingServer();
    Thread.ofVirtual().start(zkTestingServer).join();
    return zkTestingServer.zkServer;
  }

  private static class ZKTestingServer implements Runnable {
    public TestingServer zkServer;

    @Override
    public void run() {
      try {
        zkServer = new TestingServer();
      } catch (Exception e) {
        fail("Failed to start ZK server", e);
      }
    }
  }
}
