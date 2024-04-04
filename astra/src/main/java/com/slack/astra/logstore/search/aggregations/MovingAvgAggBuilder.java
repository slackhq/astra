package com.slack.astra.logstore.search.aggregations;

import java.util.Objects;

public class MovingAvgAggBuilder extends PipelineAggBuilder {
  public static final String TYPE = "moving_avg";
  private final String model;
  private final Integer window;
  private final Integer predict;
  private Double alpha;
  private Double beta;
  private Double gamma;
  private Integer period;
  private boolean pad;
  private boolean minimize;

  public MovingAvgAggBuilder(
      String name, String bucketsPath, String model, Integer window, Integer predict) {
    super(name, bucketsPath);

    this.model = model;
    this.window = window;
    this.predict = predict;
  }

  public MovingAvgAggBuilder(
      String name,
      String bucketsPath,
      String model,
      Integer window,
      Integer predict,
      Double alpha,
      boolean minimize) {
    this(name, bucketsPath, model, window, predict);

    this.alpha = alpha;
    this.minimize = minimize;
  }

  public MovingAvgAggBuilder(
      String name,
      String bucketsPath,
      String model,
      Integer window,
      Integer predict,
      Double alpha,
      Double beta,
      boolean minimize) {
    this(name, bucketsPath, model, window, predict, alpha, minimize);

    this.beta = beta;
  }

  public MovingAvgAggBuilder(
      String name,
      String bucketsPath,
      String model,
      Integer window,
      Integer predict,
      Double alpha,
      Double beta,
      Double gamma,
      Integer period,
      boolean pad,
      boolean minimize) {
    this(name, bucketsPath, model, window, predict, alpha, beta, minimize);

    this.gamma = gamma;
    this.period = period;
    this.pad = pad;
  }

  public String getModel() {
    return model;
  }

  public Integer getWindow() {
    return window;
  }

  public Integer getPredict() {
    return predict;
  }

  public Double getAlpha() {
    return alpha;
  }

  public Double getBeta() {
    return beta;
  }

  public Double getGamma() {
    return gamma;
  }

  public Integer getPeriod() {
    return period;
  }

  public boolean isPad() {
    return pad;
  }

  public boolean isMinimize() {
    return minimize;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    MovingAvgAggBuilder that = (MovingAvgAggBuilder) o;

    if (pad != that.pad) return false;
    if (minimize != that.minimize) return false;
    if (!model.equals(that.model)) return false;
    if (!Objects.equals(window, that.window)) return false;
    if (!Objects.equals(predict, that.predict)) return false;
    if (!Objects.equals(alpha, that.alpha)) return false;
    if (!Objects.equals(beta, that.beta)) return false;
    if (!Objects.equals(gamma, that.gamma)) return false;
    return Objects.equals(period, that.period);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + model.hashCode();
    result = 31 * result + (window != null ? window.hashCode() : 0);
    result = 31 * result + (predict != null ? predict.hashCode() : 0);
    result = 31 * result + (alpha != null ? alpha.hashCode() : 0);
    result = 31 * result + (beta != null ? beta.hashCode() : 0);
    result = 31 * result + (gamma != null ? gamma.hashCode() : 0);
    result = 31 * result + (period != null ? period.hashCode() : 0);
    result = 31 * result + (pad ? 1 : 0);
    result = 31 * result + (minimize ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "MovingAvgAggBuilder{"
        + "model='"
        + model
        + '\''
        + ", window="
        + window
        + ", predict="
        + predict
        + ", alpha="
        + alpha
        + ", beta="
        + beta
        + ", gamma="
        + gamma
        + ", period="
        + period
        + ", pad="
        + pad
        + ", minimize="
        + minimize
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
