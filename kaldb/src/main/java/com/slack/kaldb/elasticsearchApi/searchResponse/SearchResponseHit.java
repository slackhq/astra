package com.slack.kaldb.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchResponseHit {

  @JsonProperty("_index")
  private final String index;

  @JsonProperty("_type")
  private final String type;

  @JsonProperty("_id")
  private final String id;

  @JsonProperty("_score")
  private final String score;

  @JsonProperty("_source")
  private final Map<String, Object> source;

  @JsonProperty("sort")
  private List<Long> sort;

  public SearchResponseHit(
      String index,
      String type,
      String id,
      String score,
      Map<String, Object> source,
      List<Long> sort) {
    this.index = index;
    this.type = type;
    this.id = id;
    this.score = score;
    this.source = source;
    this.sort = sort;
  }

  public String getIndex() {
    return index;
  }

  public String getType() {
    return type;
  }

  public String getId() {
    return id;
  }

  public String getScore() {
    return score;
  }

  public Map<String, Object> getSource() {
    return source;
  }

  public List<Long> getSort() {
    return sort;
  }

  public static class Builder {
    private String index;
    private String type;
    private String id;
    private String score;
    private Map<String, Object> source = new HashMap<>();
    private List<Long> sort = new ArrayList<>();

    public Builder index(String index) {
      this.index = index;
      return this;
    }

    public Builder type(String type) {
      this.type = type;
      return this;
    }

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public Builder score(String score) {
      this.score = score;
      return this;
    }

    public Builder source(Map<String, Object> source) {
      this.source = source;
      return this;
    }

    public Builder sort(List<Long> sort) {
      this.sort = sort;
      return this;
    }

    public SearchResponseHit build() {
      return new SearchResponseHit(
          this.index, this.type, this.id, this.score, this.source, this.sort);
    }
  }

  public static SearchResponseHit fromByteString(ByteString byteString) throws IOException {
    LogWireMessage hit = JsonUtil.read(byteString.toStringUtf8(), LogWireMessage.class);
    LogMessage message = LogMessage.fromWireMessage(hit);

    return new SearchResponseHit.Builder()
        .index(message.getIndex())
        .type("_doc")
        .source(message.getSource())
        .sort(ImmutableList.of(message.getMillisecondsSinceEpoch()))
        .build();
  }
}
