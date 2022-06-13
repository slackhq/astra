package com.slack.kaldb.logstore.search;

import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.logstore.LogMessage.SystemField;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

public class StatsCollector extends SimpleCollector {

  public final Histogram histogram;
  private NumericDocValues docValues;
  public int count;

  public StatsCollector(Histogram histogram) {
    this.histogram = histogram;
    docValues = null;
  }

  @Override
  protected void doSetNextReader(final LeafReaderContext context) throws IOException {
    docValues = context.reader().getNumericDocValues(SystemField.TIME_SINCE_EPOCH.fieldName);
  }

  public Histogram getHistogram() {
    return histogram;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public void collect(int doc) throws IOException {
    if (docValues != null && docValues.advanceExact(doc)) {
      long timestamp = docValues.longValue();
      histogram.add(timestamp);
    }
  }
}
