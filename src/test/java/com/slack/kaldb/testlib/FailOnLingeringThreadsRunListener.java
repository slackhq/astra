package com.slack.kaldb.testlib;

import java.util.Collections;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.runner.Description;
import org.junit.runner.Result;

public class FailOnLingeringThreadsRunListener extends org.junit.runner.notification.RunListener {
  private Set<Thread> threadsBefore;

  @Override
  public synchronized void testRunStarted(Description description) throws Exception {
    threadsBefore = takePhoto();
    super.testRunStarted(description);
  }

  @Override
  public synchronized void testRunFinished(Result result) throws Exception {
    super.testRunFinished(result);

    Set<Thread> threadsAfter = spotTheDiffs(threadsBefore);

    // only complain on success, as failures may have caused cleanup code not to run...
    if (result.wasSuccessful()) {
      if (!threadsAfter.isEmpty())
        throw new IllegalStateException("Lingering threads in test: " + threadsAfter);
    }
  }

  public static Set<Thread> takePhoto() {

    return Collections.unmodifiableSet(Thread.getAllStackTraces().keySet());
  }

  @AfterClass
  public static Set<Thread> spotTheDiffs(Set<Thread> threadsBefore) {
    Set<Thread> threadsAfter = Thread.getAllStackTraces().keySet();
    if (threadsAfter.size() != threadsBefore.size()) {
      threadsAfter.removeAll(threadsBefore);
      return Collections.unmodifiableSet(threadsAfter);
    }

    return Collections.emptySet();
  }
}
