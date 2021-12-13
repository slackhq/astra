package com.slack.kaldb.util;

import com.google.common.util.concurrent.FutureCallback;
import java.util.concurrent.atomic.AtomicInteger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FutureUtils {
  /** Uses the provided atomic integer to keep track of FutureCallbacks that are successful */
  public static com.google.common.util.concurrent.FutureCallback<Object> successCountingCallback(
      AtomicInteger counter) {
    return new FutureCallback<>() {
      @Override
      public void onSuccess(@Nullable Object result) {
        counter.incrementAndGet();
      }

      @Override
      public void onFailure(@NonNull Throwable t) {
        // no-op
      }
    };
  }
}
