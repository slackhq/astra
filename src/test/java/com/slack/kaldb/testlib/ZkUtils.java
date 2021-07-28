package com.slack.kaldb.testlib;

import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.ZooKeeper;

public class ZkUtils {
  /**
   * When using testingServer.close() this ensures that we do not get stuck with infinite socket
   * retries (ie, Session 0x0 for sever localhost/127.0.0.1:55733, Closing socket connection.
   * Attempting reconnect except it is a SessionExpiredException.)
   *
   * @see <a
   *     href="https://stackoverflow.com/questions/61781371/wait-for-zookeeper-client-threads-to-stop">Wait
   *     for Zookeeper client threads to stop</a>
   * @see <a
   *     href="https://stackoverflow.com/questions/68215630/why-isnt-curator-recovering-when-zookeeper-is-back-online">Why
   *     isn't curator recovering when zookeeper is back online?</a>
   * @see <a href="https://github.com/apache/curator/pull/391">CURATOR-599 Configurable
   *     ZookeeperFactory by ZKClientConfig</a>
   */
  public static void closeZookeeperClientConnection(ZooKeeper zooKeeper)
      throws NoSuchFieldException, IllegalAccessException, IOException {
    Field cnField = ZooKeeper.class.getDeclaredField("cnxn");
    cnField.setAccessible(true);
    ClientCnxn cnxn = (ClientCnxn) cnField.get(zooKeeper);
    cnxn.close();
  }
}
