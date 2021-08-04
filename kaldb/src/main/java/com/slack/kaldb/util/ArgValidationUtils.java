package com.slack.kaldb.util;

public class ArgValidationUtils {
  public static void ensureNonEmptyString(String s, String exceptionMessage) {
    if (s == null || s.isEmpty()) {
      throw new IllegalArgumentException(exceptionMessage);
    }
  }

  public static void ensureNonNullString(String s, String exceptionMessage) {
    if (s == null) {
      throw new IllegalArgumentException(exceptionMessage);
    }
  }

  public static void ensureTrue(boolean condition, String exceptionMessage) {
    if (!condition) {
      throw new IllegalArgumentException(exceptionMessage);
    }
  }
}
