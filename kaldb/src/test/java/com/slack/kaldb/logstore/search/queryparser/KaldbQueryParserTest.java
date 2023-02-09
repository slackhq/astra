package com.slack.kaldb.logstore.search.queryparser;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;

public class KaldbQueryParserTest {
  @Test
  public void testInit() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new KaldbQueryParser("test", new StandardAnalyzer(), null));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new KaldbQueryParser("test", new StandardAnalyzer(), new ConcurrentHashMap<>()));
  }
}
