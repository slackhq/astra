package com.slack.astra.zipkinApi;

import static com.slack.astra.zipkinApi.ZipkinService.TRACE_CACHE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class ZipkinServiceTest {
  private static final String TEST_S3_BUCKET = "zipkin-service-test";
  @Mock private AstraQueryServiceBase searcher;
  private ZipkinService zipkinService;
  private AstraSearch.SearchResult mockSearchResult;
  private AstraSearch.SearchResult mockEmptySearchResult;
  private BlobStore mockBlobStore;

  private static final int defaultMaxSpans = 2000;
  private static final int defaultLookbackMins = 60 * 24 * 7;

  private static final long defaultDataFreshnessInMinutes = 5;

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(TEST_S3_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  @BeforeEach
  public void setup() throws IOException {
    MockitoAnnotations.openMocks(this);
    S3AsyncClient s3AsyncClient =
        S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    BlobStore blobStore = new BlobStore(s3AsyncClient, TEST_S3_BUCKET);
    mockBlobStore = spy(blobStore);
    zipkinService =
        spy(
            new ZipkinService(
                searcher,
                mockBlobStore,
                defaultMaxSpans,
                defaultLookbackMins,
                defaultDataFreshnessInMinutes));
    // Build mockSearchResult
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode =
        objectMapper.readTree(Resources.getResource("zipkinApi/search_result.json"));
    String jsonString = objectMapper.writeValueAsString(jsonNode);
    AstraSearch.SearchResult.Builder builder = AstraSearch.SearchResult.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    mockSearchResult = builder.build();

    // Build mockEmptySearchResult
    objectMapper = new ObjectMapper();
    jsonNode = objectMapper.readTree(Resources.getResource("zipkinApi/empty_search_result.json"));
    jsonString = objectMapper.writeValueAsString(jsonNode);
    builder = AstraSearch.SearchResult.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    mockEmptySearchResult = builder.build();
  }

  @Test
  public void testGetTraceByTraceId_onlyTraceIdProvided() throws Exception {

    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);
      String traceId = "test_trace_1";

      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      // Act
      HttpResponse response =
          zipkinService.getTraceByTraceId(
              traceId,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

      // Assert
      assertEquals(HttpStatus.OK, aggregatedResponse.status());
    }
  }

  @Test
  public void testGetTraceByTraceId_respectsDefaultMaxSpans() throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_2";
      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      zipkinService.getTraceByTraceId(
          traceId,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());

      verify(searcher)
          .doSearch(
              Mockito.argThat(
                  request ->
                      request.getHowMany() == defaultMaxSpans
                          && request.getQuery().contains("\"trace_id\":\"" + traceId + "\"")));
    }
  }

  @Test
  public void testGetTraceByTraceId_respectsMaxSpans() throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_2";
      when(searcher.doSearch(any())).thenReturn(mockSearchResult);
      int maxSpansParam = 10000;

      zipkinService.getTraceByTraceId(
          traceId,
          Optional.empty(),
          Optional.empty(),
          Optional.of(maxSpansParam),
          Optional.empty(),
          Optional.empty());

      verify(searcher)
          .doSearch(
              Mockito.argThat(
                  request ->
                      request.getHowMany() == maxSpansParam
                          && request.getQuery().contains("\"trace_id\":\"" + traceId + "\"")));
    }
  }

  @Test
  public void testGetTraceByTraceId_respectUserRequest_upload_after_search() throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_3";
      String traceFilePath = String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId);
      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      boolean userRequest = true;

      HttpResponse response =
          zipkinService.getTraceByTraceId(
              traceId,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(userRequest),
              Optional.empty());

      verify(searcher)
          .doSearch(
              Mockito.argThat(
                  request -> request.getQuery().contains("\"trace_id\":\"" + traceId + "\"")));

      verify(mockBlobStore).uploadData(anyString(), anyString(), eq(true));
      verify(mockBlobStore).copyFile(anyString(), eq(traceFilePath));
      verify(mockBlobStore).delete(anyString());

      assertNotNull(response, "Response should not be null");
      response
          .aggregate()
          .thenAccept(
              aggregatedResponse -> {
                assertEquals(HttpStatus.OK, aggregatedResponse.status());
                assertEquals(mockSearchResult.toString(), aggregatedResponse.contentUtf8());
              });

      String returnData = mockBlobStore.readFileData(traceFilePath, true);
      assertNotNull(returnData, "Decompressed data should not be null");
    }
  }

  @Test
  public void testGetTraceByTraceId_respectUserRequest_no_upload_after_search_empty_search_result()
      throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_4";
      String traceFilePath = String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId);
      when(searcher.doSearch(any())).thenReturn(mockEmptySearchResult);

      boolean userRequest = true;

      HttpResponse response =
          zipkinService.getTraceByTraceId(
              traceId,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(userRequest),
              Optional.empty());

      verify(searcher)
          .doSearch(
              Mockito.argThat(
                  request -> request.getQuery().contains("\"trace_id\":\"" + traceId + "\"")));

      verify(mockBlobStore, never()).uploadData(anyString(), anyString(), eq(true));
      verify(mockBlobStore, never()).copyFile(anyString(), eq(traceFilePath));
      verify(mockBlobStore, never()).delete(anyString());

      assertNotNull(response, "Response should not be null");
      response
          .aggregate()
          .thenAccept(
              aggregatedResponse -> {
                assertEquals(HttpStatus.OK, aggregatedResponse.status());
                assertEquals(mockSearchResult.toString(), aggregatedResponse.contentUtf8());
              });
    }
  }

  @Test
  public void testGetTraceByTraceId_respectUserRequest_skip_search() throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_5";
      String traceFilePath = String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId);

      Path filePath = Paths.get(Resources.getResource("zipkinApi/traceData.json").toURI());

      mockBlobStore.uploadData(traceFilePath, Files.readString(filePath), true);

      boolean userRequest = true;

      HttpResponse response =
          zipkinService.getTraceByTraceId(
              traceId,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(userRequest),
              Optional.empty());

      verify(mockBlobStore).readFileData(eq(traceFilePath), eq(true));
      verify(searcher, never()).doSearch(Mockito.any());
      assertNotNull(response, "Response should not be null");
      response
          .aggregate()
          .thenAccept(
              aggregatedResponse -> {
                assertEquals(HttpStatus.OK, aggregatedResponse.status());
                assertEquals(mockSearchResult.toString(), aggregatedResponse.contentUtf8());
              });

      String returnData = mockBlobStore.readFileData(traceFilePath, true);
      assertNotNull(returnData, "Decompressed data should not be null");
    }
  }

  @Test
  public void testGetTraceByTraceId_respectUserRequest_respectDataFreshness_perform_search()
      throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_6";
      String traceFilePath = String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId);

      boolean userRequest = true;
      long dataFreshnessInSeconds =
          Instant.now().getEpochSecond()
              - Instant.parse("2024-10-31T20:48:33.560Z").getEpochSecond()
              + 100;

      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      HttpResponse response =
          zipkinService.getTraceByTraceId(
              traceId,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(userRequest),
              Optional.of(dataFreshnessInSeconds));

      verify(mockBlobStore).readFileData(traceFilePath, true);
      verify(mockBlobStore, never()).uploadData(anyString(), anyString(), eq(true));
      verify(mockBlobStore, never()).copyFile(anyString(), eq(traceFilePath));
      verify(mockBlobStore, never()).delete(anyString());
      verify(searcher)
          .doSearch(
              Mockito.argThat(
                  request -> request.getQuery().contains("\"trace_id\":\"" + traceId + "\"")));
      assertNotNull(response, "Response should not be null");
      response
          .aggregate()
          .thenAccept(
              aggregatedResponse -> {
                assertEquals(HttpStatus.OK, aggregatedResponse.status());
                assertEquals(mockSearchResult.toString(), aggregatedResponse.contentUtf8());
              });

      Path directoryDownloaded = Files.createTempDirectory(traceId);
      mockBlobStore.download(
          String.format("%s/%s", TRACE_CACHE_PREFIX, traceId), directoryDownloaded);

      File[] filesDownloaded = directoryDownloaded.toFile().listFiles();
      assertThat(Objects.requireNonNull(filesDownloaded).length).isEqualTo(0);
    }
  }

  @Test
  public void
      testGetTraceByTraceId_respectUserRequest_respectDataFreshness_perform_search_and_save()
          throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_7";
      String traceFilePath = String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId);
      boolean userRequest = true;
      long dataFreshnessInSeconds = 100;

      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      HttpResponse response =
          zipkinService.getTraceByTraceId(
              traceId,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.of(userRequest),
              Optional.of(dataFreshnessInSeconds));

      verify(mockBlobStore).readFileData(traceFilePath, true);
      verify(searcher)
          .doSearch(
              Mockito.argThat(
                  request -> request.getQuery().contains("\"trace_id\":\"" + traceId + "\"")));

      verify(mockBlobStore).uploadData(anyString(), anyString(), eq(true));
      verify(mockBlobStore).copyFile(anyString(), eq(traceFilePath));
      verify(mockBlobStore).delete(anyString());
      assertNotNull(response, "Response should not be null");
      response
          .aggregate()
          .thenAccept(
              aggregatedResponse -> {
                assertEquals(HttpStatus.OK, aggregatedResponse.status());
                assertEquals(mockSearchResult.toString(), aggregatedResponse.contentUtf8());
              });

      String returnData = mockBlobStore.readFileData(traceFilePath, true);

      assertNotNull(returnData, "Decompressed data should not be null");
    }
  }

  @Test
  public void testRetrieveDataFromS3() {
    // Arrange
    String traceId = "test_trace_123";
    String jsonData =
        "[{\"id\":\"101\",\"traceId\":\"test_trace_123\",\"name\":\"test-span\",\"tags\":{\"key1\":\"value1\",\"key2\":\"value2\"}}]";
    zipkinService.saveDataToBlobStoreCache(traceId, jsonData);

    // Act
    String retrievedData = zipkinService.retrieveDataFromBlobStoreCache(traceId);

    // Assert
    assertNotNull(retrievedData, "Retrieved data should not be null");
    assertEquals(jsonData, retrievedData, "Retrieved data should match original");
  }

  @Test
  public void testRetrieveDataFromS3_nullTraceId_throwsException() {
    // Act & Assert
    try {
      zipkinService.retrieveDataFromBlobStoreCache(null);
      fail("Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for null traceId
    }
  }

  @Test
  public void testRetrieveDataFromS3_emptyTraceId_throwsException() {
    // Act & Assert
    try {
      zipkinService.retrieveDataFromBlobStoreCache("");
      fail("Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for empty traceId
    }
  }

  @Test
  public void testSaveDataToS3() {
    // Arrange
    String traceId = "test_trace_456";
    String jsonData =
        "[{\"id\":\"101\",\"traceId\":\"test_trace_456\",\"name\":\"test-span\",\"tags\":{\"key1\":\"value1\",\"key2\":\"value2\"}}]";

    // Act
    zipkinService.saveDataToBlobStoreCache(traceId, jsonData);

    // Assert
    verify(mockBlobStore).uploadData(anyString(), anyString(), eq(true));
    verify(mockBlobStore).copyFile(anyString(), anyString());
    verify(mockBlobStore).delete(anyString());

    // Assert
    String fileData =
        mockBlobStore.readFileData(
            String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId), true);
    assertEquals(jsonData, fileData, "Decompressed data should match original");
  }

  @Test
  public void testSaveDataToS3_nullTraceId_throwsException() {
    String jsonData = "[{\"id\":\"101\"}]";

    // Act & Assert
    try {
      zipkinService.saveDataToBlobStoreCache(null, jsonData);
      assertTrue(false, "Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for null traceId
    }
  }

  @Test
  public void testSaveDataToS3_emptyTraceId_throwsException() {
    // Arrange
    String jsonData = "[{\"id\":\"101\"}]";

    // Act & Assert
    try {
      zipkinService.saveDataToBlobStoreCache("", jsonData);
      assertTrue(false, "Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for empty traceId
    }
  }

  @Test
  public void testSaveDataToS3_nullData_throwsException() {

    // Act & Assert
    try {
      zipkinService.saveDataToBlobStoreCache("test_trace", null);
      assertTrue(false, "Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for null data
    }
  }
}
