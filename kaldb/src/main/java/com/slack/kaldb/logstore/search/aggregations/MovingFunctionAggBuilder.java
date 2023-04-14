package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MovingFunctionAggBuilder extends PipelineAggBuilder {
  public static final String TYPE = "moving_fn";
  private final Integer shift;
  private final int window;
  private final String script;

  public MovingFunctionAggBuilder(
      String name, String bucketsPath, String script, int window, Integer shift) {
    super(name, Map.of(), List.of(), bucketsPath);
    this.shift = shift;
    this.window = window;
    this.script = script;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  public Integer getShift() {
    return shift;
  }

  public int getWindow() {
    return window;
  }

  public String getScript() {
    return script;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MovingFunctionAggBuilder)) return false;
    if (!super.equals(o)) return false;

    MovingFunctionAggBuilder that = (MovingFunctionAggBuilder) o;

    if (window != that.window) return false;
    if (!Objects.equals(shift, that.shift)) return false;
    return script.equals(that.script);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (shift != null ? shift.hashCode() : 0);
    result = 31 * result + window;
    result = 31 * result + script.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "MovingFunctionAggBuilder{"
        + "shift="
        + shift
        + ", window="
        + window
        + ", script='"
        + script
        + '\''
        + ", bucketsPath='"
        + bucketsPath
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
