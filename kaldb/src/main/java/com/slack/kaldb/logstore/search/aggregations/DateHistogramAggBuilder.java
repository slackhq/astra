package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Aggregation request type to form a histogram bucketed by a timestamp */
public class DateHistogramAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "date_histogram";
  private final String interval;
  private final String offset;
  private final String zoneId;
  private final long minDocCount;

  private final String format;

  private final Map<String, Long> extendedBounds;

  public DateHistogramAggBuilder(String name, String fieldName, String interval) {
    super(name, fieldName);

    this.interval = interval;
    this.offset = "";
    this.zoneId = null;
    this.minDocCount = 1;
    this.format = null;
    this.extendedBounds = Map.of();
  }

  public DateHistogramAggBuilder(
      String name,
      String fieldName,
      String interval,
      String offset,
      String zoneId,
      long minDocCount,
      String format,
      Map<String, Long> extendedBounds,
      List<AggBuilder> subAggregations) {
    // todo - metadata?
    super(name, Map.of(), subAggregations, fieldName);

    this.interval = interval;
    this.offset = offset;
    this.zoneId = zoneId;
    this.minDocCount = minDocCount;
    this.format = format;
    this.extendedBounds = extendedBounds;
  }

  public String getInterval() {
    return interval;
  }

  public String getZoneId() {
    return zoneId;
  }

  public String getOffset() {
    return offset;
  }

  public long getMinDocCount() {
    return minDocCount;
  }

  public String getFormat() {
    return format;
  }

  public Map<String, Long> getExtendedBounds() {
    return extendedBounds;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DateHistogramAggBuilder that)) return false;
    if (!super.equals(o)) return false;

    if (minDocCount != that.minDocCount) return false;
    if (!interval.equals(that.interval)) return false;
    if (!Objects.equals(offset, that.offset)) return false;
    if (!Objects.equals(zoneId, that.zoneId)) return false;
    if (!Objects.equals(format, that.format)) return false;
    return Objects.equals(extendedBounds, that.extendedBounds);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + interval.hashCode();
    result = 31 * result + (offset != null ? offset.hashCode() : 0);
    result = 31 * result + (zoneId != null ? zoneId.hashCode() : 0);
    result = 31 * result + (int) (minDocCount ^ (minDocCount >>> 32));
    result = 31 * result + (format != null ? format.hashCode() : 0);
    result = 31 * result + (extendedBounds != null ? extendedBounds.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DateHistogramAggBuilder{"
        + "interval='"
        + interval
        + '\''
        + ", offset='"
        + offset
        + '\''
        + ", zoneId='"
        + zoneId
        + '\''
        + ", minDocCount="
        + minDocCount
        + ", format='"
        + format
        + '\''
        + ", extendedBounds="
        + extendedBounds
        + ", field='"
        + field
        + '\''
        + ", missing="
        + missing
        + ", script='"
        + script
        + '\''
        + ", name='"
        + name
        + '\''
        + ", metadata="
        + metadata
        + ", subAggregations="
        + subAggregations
        + '}';
  }
}
