package com.slack.kaldb.zipkinApi;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonFormat(shape = JsonFormat.Shape.ARRAY)
public class GetServiceResult {
  private final List<String> services;

  public GetServiceResult(List<String> services) {
    this.services = services;
  }

  @JsonProperty("services")
  public List<String> getServices() {
    return services;
  }
}
