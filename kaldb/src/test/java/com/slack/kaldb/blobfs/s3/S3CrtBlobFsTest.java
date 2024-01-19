package com.slack.kaldb.blobfs.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3CrtBlobFsTest {
  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder().silent().withSecureConnection(false).build();

  final String DELIMITER = "/";
  final String SCHEME = "s3";
  final String FILE_FORMAT = "%s://%s/%s";
  final String DIR_FORMAT = "%s://%s";

  private final S3AsyncClient s3Client =
      S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
  private String bucket;
  private S3CrtBlobFs s3BlobFs;

  @BeforeEach
  public void setUp() throws ExecutionException, InterruptedException {
    bucket = "test-bucket-" + UUID.randomUUID();
    s3BlobFs = new S3CrtBlobFs(s3Client);
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).get();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (s3BlobFs != null) {
      s3BlobFs.close();
    }
  }

  private void createEmptyFile(String folderName, String fileName)
      throws ExecutionException, InterruptedException {
    String fileNameWithFolder = folderName + DELIMITER + fileName;
    if (folderName.isEmpty()) {
      fileNameWithFolder = fileName;
    }
    s3Client
        .putObject(
            S3TestUtils.getPutObjectRequest(bucket, fileNameWithFolder),
            AsyncRequestBody.fromBytes(new byte[0]))
        .get();
  }

  @Test
  public void testTouchFileInBucket() throws Exception {

    String[] originalFiles = new String[] {"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      s3BlobFs.touch(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName)));
    }
    ListObjectsV2Response listObjectsV2Response =
        s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(bucket, "", true)).get();

    String[] response =
        listObjectsV2Response.contents().stream()
            .map(S3Object::key)
            .filter(x -> x.contains("touch"))
            .toArray(String[]::new);

    assertEquals(response.length, originalFiles.length);
    assertTrue(Arrays.equals(response, originalFiles));
  }

  @Test
  public void testTouchFilesInFolder() throws Exception {

    String folder = "my-files";
    String[] originalFiles = new String[] {"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      String fileNameWithFolder = folder + DELIMITER + fileName;
      s3BlobFs.touch(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileNameWithFolder)));
    }
    ListObjectsV2Response listObjectsV2Response =
        s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(bucket, folder, false)).get();

    String[] response =
        listObjectsV2Response.contents().stream()
            .map(S3Object::key)
            .filter(x -> x.contains("touch"))
            .toArray(String[]::new);
    assertEquals(response.length, originalFiles.length);

    assertTrue(
        Arrays.equals(
            response, Arrays.stream(originalFiles).map(x -> folder + DELIMITER + x).toArray()));
  }

  @Test
  public void testListFilesInBucketNonRecursive() throws Exception {
    String[] originalFiles = new String[] {"a-list.txt", "b-list.txt", "c-list.txt"};
    List<String> expectedFileNames = new ArrayList<>();

    for (String fileName : originalFiles) {
      createEmptyFile("", fileName);
      expectedFileNames.add(String.format(FILE_FORMAT, SCHEME, bucket, fileName));
    }

    String[] actualFiles =
        s3BlobFs.listFiles(URI.create(String.format(DIR_FORMAT, SCHEME, bucket)), false);

    actualFiles = Arrays.stream(actualFiles).filter(x -> x.contains("list")).toArray(String[]::new);
    assertEquals(actualFiles.length, originalFiles.length);

    assertTrue(Arrays.equals(actualFiles, expectedFileNames.toArray()));
  }

  @Test
  public void testListFilesInFolderNonRecursive() throws Exception {
    String folder = "list-files";
    String[] originalFiles = new String[] {"a-list-2.txt", "b-list-2.txt", "c-list-2.txt"};

    for (String fileName : originalFiles) {
      createEmptyFile(folder, fileName);
    }

    String[] actualFiles =
        s3BlobFs.listFiles(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, folder)), false);

    actualFiles =
        Arrays.stream(actualFiles).filter(x -> x.contains("list-2")).toArray(String[]::new);
    assertEquals(actualFiles.length, originalFiles.length);

    assertTrue(
        Arrays.equals(
            Arrays.stream(originalFiles)
                .map(
                    fileName ->
                        String.format(FILE_FORMAT, SCHEME, bucket, folder + DELIMITER + fileName))
                .toArray(),
            actualFiles));
  }

  @Test
  public void testListFilesInFolderRecursive() throws Exception {
    String folder = "list-files-rec";
    String[] nestedFolders = new String[] {"list-files-child-1", "list-files-child-2"};
    String[] originalFiles = new String[] {"a-list-3.txt", "b-list-3.txt", "c-list-3.txt"};

    List<String> expectedResultList = new ArrayList<>();
    for (String childFolder : nestedFolders) {
      String folderName = folder + DELIMITER + childFolder;
      for (String fileName : originalFiles) {
        createEmptyFile(folderName, fileName);
        expectedResultList.add(
            String.format(FILE_FORMAT, SCHEME, bucket, folderName + DELIMITER + fileName));
      }
    }
    String[] actualFiles =
        s3BlobFs.listFiles(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, folder)), true);

    actualFiles =
        Arrays.stream(actualFiles).filter(x -> x.contains("list-3")).toArray(String[]::new);
    assertEquals(actualFiles.length, expectedResultList.size());
    assertTrue(Arrays.equals(expectedResultList.toArray(), actualFiles));
  }

  @Test
  public void testDeleteFile() throws Exception {
    String[] originalFiles = new String[] {"a-delete.txt", "b-delete.txt", "c-delete.txt"};
    String fileToDelete = "a-delete.txt";

    List<String> expectedResultList = new ArrayList<>();
    for (String fileName : originalFiles) {
      createEmptyFile("", fileName);
      if (!fileName.equals(fileToDelete)) {
        expectedResultList.add(fileName);
      }
    }

    s3BlobFs.delete(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileToDelete)), false);

    ListObjectsV2Response listObjectsV2Response =
        s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(bucket, "", true)).get();
    String[] actualResponse =
        listObjectsV2Response.contents().stream()
            .map(S3Object::key)
            .filter(x -> x.contains("delete"))
            .toArray(String[]::new);

    assertEquals(actualResponse.length, 2);
    assertTrue(Arrays.equals(actualResponse, expectedResultList.toArray()));
  }

  @Test
  public void testDeleteFolder() throws Exception {
    String[] originalFiles = new String[] {"a-delete-2.txt", "b-delete-2.txt", "c-delete-2.txt"};
    String folderName = "my-files";

    for (String fileName : originalFiles) {
      createEmptyFile(folderName, fileName);
    }

    s3BlobFs.delete(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, folderName)), true);

    ListObjectsV2Response listObjectsV2Response =
        s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(bucket, "", true)).get();
    String[] actualResponse =
        listObjectsV2Response.contents().stream()
            .map(S3Object::key)
            .filter(x -> x.contains("delete-2"))
            .toArray(String[]::new);

    assertEquals(0, actualResponse.length);
  }

  @Test
  public void testIsDirectory() throws Exception {
    String[] originalFiles = new String[] {"a-dir.txt", "b-dir.txt", "c-dir.txt"};
    String folder = "my-files-dir";
    String childFolder = "my-files-dir-child";
    for (String fileName : originalFiles) {
      String folderName = folder + DELIMITER + childFolder;
      createEmptyFile(folderName, fileName);
    }

    boolean isBucketDir =
        s3BlobFs.isDirectory(URI.create(String.format(DIR_FORMAT, SCHEME, bucket)));
    boolean isDir =
        s3BlobFs.isDirectory(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, folder)));
    boolean isDirChild =
        s3BlobFs.isDirectory(
            URI.create(
                String.format(FILE_FORMAT, SCHEME, bucket, folder + DELIMITER + childFolder)));
    boolean notIsDir =
        s3BlobFs.isDirectory(
            URI.create(
                String.format(
                    FILE_FORMAT,
                    SCHEME,
                    bucket,
                    folder + DELIMITER + childFolder + DELIMITER + "a-delete.txt")));

    assertTrue(isBucketDir);
    assertTrue(isDir);
    assertTrue(isDirChild);
    assertFalse(notIsDir);
  }

  @Test
  public void testExists() throws Exception {
    String[] originalFiles = new String[] {"a-ex.txt", "b-ex.txt", "c-ex.txt"};
    String folder = "my-files-dir";
    String childFolder = "my-files-dir-child";

    for (String fileName : originalFiles) {
      String folderName = folder + DELIMITER + childFolder;
      createEmptyFile(folderName, fileName);
    }

    boolean bucketExists = s3BlobFs.exists(URI.create(String.format(DIR_FORMAT, SCHEME, bucket)));
    boolean dirExists =
        s3BlobFs.exists(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, folder)));
    boolean childDirExists =
        s3BlobFs.exists(
            URI.create(
                String.format(FILE_FORMAT, SCHEME, bucket, folder + DELIMITER + childFolder)));
    boolean fileExists =
        s3BlobFs.exists(
            URI.create(
                String.format(
                    FILE_FORMAT,
                    SCHEME,
                    bucket,
                    folder + DELIMITER + childFolder + DELIMITER + "a-ex.txt")));
    boolean fileNotExists =
        s3BlobFs.exists(
            URI.create(
                String.format(
                    FILE_FORMAT,
                    SCHEME,
                    bucket,
                    folder + DELIMITER + childFolder + DELIMITER + "d-ex.txt")));

    assertTrue(bucketExists);
    assertTrue(dirExists);
    assertTrue(childDirExists);
    assertTrue(fileExists);
    assertFalse(fileNotExists);
  }

  @Test
  public void testCopyFromAndToLocal() throws Exception {
    String fileName = "copyFile.txt";

    File fileToCopy = new File(getClass().getClassLoader().getResource(fileName).getFile());

    s3BlobFs.copyFromLocalFile(
        fileToCopy, URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName)));

    HeadObjectResponse headObjectResponse =
        s3Client.headObject(S3TestUtils.getHeadObjectRequest(bucket, fileName)).get();

    assertEquals(headObjectResponse.contentLength(), (Long) fileToCopy.length());

    File fileToDownload = new File("copyFile_download_crt.txt").getAbsoluteFile();
    s3BlobFs.copyToLocalFile(
        URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName)), fileToDownload);
    assertEquals(fileToCopy.length(), fileToDownload.length());

    fileToDownload.deleteOnExit();
  }

  @Test
  public void testCopyFromAndToLocalDirectory() throws Exception {
    String fileName = "copyFile.txt";

    File fileToCopy =
        new File(Resources.getResource(String.format("s3CrtBlobFsTest/%s", fileName)).getFile());

    s3BlobFs.copyFromLocalFile(
        fileToCopy.getParentFile(), URI.create(String.format(FILE_FORMAT, SCHEME, bucket, "")));

    HeadObjectResponse headObjectResponse =
        s3Client.headObject(S3TestUtils.getHeadObjectRequest(bucket, fileName)).get();

    assertEquals(headObjectResponse.contentLength(), (Long) fileToCopy.length());

    File fileToDownload = new File(fileName).getAbsoluteFile();
    s3BlobFs.copyToLocalFile(
        URI.create(String.format(FILE_FORMAT, SCHEME, bucket, "")), fileToDownload.getParentFile());
    assertEquals(fileToCopy.length(), fileToDownload.length());

    fileToDownload.deleteOnExit();
  }

  @Test
  public void testOpenFile() throws Exception {
    String fileName = "sample.txt";
    String fileContent = "Hello, World";

    s3Client
        .putObject(
            S3TestUtils.getPutObjectRequest(bucket, fileName),
            AsyncRequestBody.fromString(fileContent))
        .get();

    InputStream is =
        s3BlobFs.open(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName)));
    String actualContents = IOUtils.toString(is, StandardCharsets.UTF_8);
    assertEquals(actualContents, fileContent);
  }

  @Test
  public void testMkdir() throws Exception {
    String folderName = "my-test-folder";

    s3BlobFs.mkdir(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, folderName)));

    HeadObjectResponse headObjectResponse =
        s3Client.headObject(S3TestUtils.getHeadObjectRequest(bucket, folderName + DELIMITER)).get();
    assertTrue(headObjectResponse.sdkHttpResponse().isSuccessful());
  }
}
