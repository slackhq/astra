package com.slack.kaldb.server;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;

public class ElasticsearchBulkIngestAPI {

  /**
   * Notes 1. Kaldb does not support the concept of "updates". It's always an add 2. Should we have
   * a concept of "index" against which the request can be made against
   */
  @Blocking
  @Post("/_bulk")
  public HttpResponse addDocument(String bulkRequest) {

    return HttpResponse.ofJson(bulkRequest);
  }
}
