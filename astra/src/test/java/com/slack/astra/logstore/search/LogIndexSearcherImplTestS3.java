package com.slack.astra.logstore.search;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.blobfs.s3.S3TestUtils;
import com.slack.astra.logstore.BlobFsUtils;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import org.apache.lucene.search.SearcherManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

class LogIndexSearcherImplTestS3 {

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder().silent().withSecureConnection(false).build();
  private final S3AsyncClient s3Client =
      S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
  //private S3CrtBlobFs s3CrtBlobFs;

  @Test
  void testS3() throws IOException {
//    S3AsyncClient s3AsyncClient =
//        S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    //s3CrtBlobFs = new S3CrtBlobFs(s3AsyncClient);

    String bucket = "test-bucket-" + UUID.randomUUID();
    //S3CrtBlobFs s3BlobFs = new S3CrtBlobFs(s3Client);
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

    String snapshotId = UUID.randomUUID().toString();

    URI s3Path = BlobFsUtils.createURI(bucket, snapshotId, "");
    SearcherManager searcherManager = LogIndexSearcherImpl.searcherManagerFromPath(Path.of(s3Path));
    ConcurrentHashMap<String, LuceneFieldDef> map = new ConcurrentHashMap<>();
    //LogIndexSearcherImpl logIndexSearcher =
    new LogIndexSearcherImpl(searcherManager, map);

  }
}
