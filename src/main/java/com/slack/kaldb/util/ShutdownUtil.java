package com.slack.kaldb.util;

public class ShutdownUtil {

  public static void fatalShutdown() {
    // TODO: Make the exit more cleaner, once the system is more stable.
    System.exit(1);
  }
}
