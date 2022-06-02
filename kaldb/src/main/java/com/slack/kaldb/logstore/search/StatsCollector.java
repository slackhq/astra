package com.slack.kaldb.logstore.search;

import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.logstore.LogMessage.SystemField;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

public class StatsCollector implements Collector {

  public final Histogram histogram;
  private NumericDocValues docValues;
  public int count;

  public StatsCollector(Histogram histogram) {
    this.histogram = histogram;
    docValues = null;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    docValues = context.reader().getNumericDocValues(SystemField.TIME_SINCE_EPOCH.fieldName);

    return new LeafCollector() {
      @Override
      public void setScorer(Scorable scorer) {}

      @Override
      public void collect(int doc) throws IOException {
        if (docValues != null && docValues.advanceExact(doc)) {
          long timestamp = docValues.longValue();
          histogram.add(timestamp);
        }
      }
    };
  }

  public Histogram getHistogram() {
    return histogram;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }
}
