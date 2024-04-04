package com.slack.astra.writer;

import com.google.common.annotations.VisibleForTesting;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared kafka functions for producers, consumers, and stream applications */
public class KafkaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

  @VisibleForTesting
  public static Properties maybeOverrideProps(
      Properties inputProps, String key, String value, boolean override) {
    Properties changedProps = (Properties) inputProps.clone();
    String userValue = changedProps.getProperty(key);
    if (userValue != null) {
      if (override) {
        LOG.warn(
            String.format(
                "Property %s is provided but will be overridden from %s to %s",
                key, userValue, value));
        changedProps.setProperty(key, value);
      } else {
        LOG.warn(
            String.format(
                "Property %s is provided but won't be overridden from %s to %s",
                key, userValue, value));
      }
    } else {
      changedProps.setProperty(key, value);
    }
    return changedProps;
  }
}
