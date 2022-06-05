package com.slack.kaldb.preprocessor.api;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Path;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.RequestConverter;
import com.linecorp.armeria.server.protobuf.ProtobufRequestConverterFunction;
import com.slack.kaldb.preprocessor.PreprocessorService;
import zipkin2.proto3.ListOfSpans;

public class OpenZipkinApiService {

  private final PreprocessorService preprocessorService;

  public OpenZipkinApiService(final PreprocessorService preprocessorService) {
    this.preprocessorService = preprocessorService;
  }

  @Post
  @Blocking
  @Path("/spans")
  @RequestConverter(ProtobufRequestConverterFunction.class)
  public HttpResponse spans(final ListOfSpans spans) {
    this.preprocessorService.addSpan(spans);
    return HttpResponse.of(HttpStatus.ACCEPTED);
  }
}
