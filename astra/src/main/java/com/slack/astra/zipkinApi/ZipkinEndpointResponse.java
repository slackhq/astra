package com.slack.astra.zipkinApi;

/**
 * Endpoint response object for Zipkin API (local, remote)
 *
 * @see <a href="https://zipkin.io/zipkin-api/#/">Zipkin API Spec</a>
 */
@SuppressWarnings("unused")
public class ZipkinEndpointResponse {
  private String serviceName;

  public ZipkinEndpointResponse() {}

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }
}
