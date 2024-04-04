package com.slack.astra.util;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This uses the Junit5 TestExecutionListener hook to provide more detailed log messages when
 * specific methods are started and finished. By default the test execution only prints out when a
 * specific class starts or finishes its test, and this also adds starting/finished notifications
 * for specific test methods as well.
 */
public class AstraTestExecutionListener implements TestExecutionListener {
  private static final Logger LOG = LoggerFactory.getLogger(AstraTestExecutionListener.class);

  @Override
  public void executionStarted(TestIdentifier testIdentifier) {
    TestExecutionListener.super.executionStarted(testIdentifier);
    String displayName = getDisplayName(testIdentifier);
    if (displayName != null) {
      LOG.info("Starting test - {}", displayName);
    }
  }

  @Override
  public void executionFinished(
      TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
    TestExecutionListener.super.executionFinished(testIdentifier, testExecutionResult);
    String displayName = getDisplayName(testIdentifier);
    if (displayName != null) {
      LOG.info("Finished test - {}", displayName);
    }
  }

  private String getDisplayName(TestIdentifier testIdentifier) {
    if (testIdentifier.getSource().isPresent()) {
      TestSource testSource = testIdentifier.getSource().get();
      if (testSource instanceof MethodSource methodSource) {
        return String.format("%s.%s", methodSource.getClassName(), methodSource.getMethodName());
      }
    }
    return null;
  }
}
