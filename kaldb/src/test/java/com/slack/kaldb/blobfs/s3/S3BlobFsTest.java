package com.slack.kaldb.blobfs.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/** Don't assertj but use junit asserts for this code to keep the blobfs lib deps simpler. */
public class S3BlobFsTest {
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  final String DELIMITER = "/";
  final String SCHEME = "s3";
  final String FILE_FORMAT = "%s://%s/%s";
  final String DIR_FORMAT = "%s://%s";

  private final S3Client s3Client = S3_MOCK_RULE.createS3ClientV2();
  private String bucket;
  private S3BlobFs s3BlobFs;

  @Before
  public void setUp() {
    bucket = "test-bucket-" + UUID.randomUUID();
    s3BlobFs = new S3BlobFs();
    s3BlobFs.init(s3Client);
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
  }

  @After
  public void tearDown() throws IOException {
    s3BlobFs.close();
  }

  private void createEmptyFile(String[] folderNames, String fileName) {
    String fileNameWithFolder = StringUtils.join(folderNames, DELIMITER) + DELIMITER + fileName;
    s3Client.putObject(
        S3TestUtils.getPutObjectRequest(bucket, fileNameWithFolder),
        RequestBody.fromBytes(new byte[0]));
  }

  private void createEmptyFile(String folderName, String fileName) {
    String fileNameWithFolder = folderName + DELIMITER + fileName;
    s3Client.putObject(
        S3TestUtils.getPutObjectRequest(bucket, fileNameWithFolder),
        RequestBody.fromBytes(new byte[0]));
  }

  @Test
  public void testTouchFileInBucket() throws Exception {

    String[] originalFiles = new String[] {"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      s3BlobFs.touch(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName)));
    }
    ListObjectsV2Response listObjectsV2Response =
        s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(bucket, "", true));

    String[] response =
        listObjectsV2Response
            .contents()
            .stream()
            .map(S3Object::key)
            .filter(x -> x.contains("touch"))
            .toArray(String[]::new);

    Assert.assertEquals(response.length, originalFiles.length);
    Assert.assertTrue(Arrays.equals(response, originalFiles));
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
        s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(bucket, folder, false));

    String[] response =
        listObjectsV2Response
            .contents()
            .stream()
            .map(S3Object::key)
            .filter(x -> x.contains("touch"))
            .toArray(String[]::new);
    Assert.assertEquals(response.length, originalFiles.length);

    Assert.assertTrue(
        Arrays.equals(
            response, Arrays.stream(originalFiles).map(x -> folder + DELIMITER + x).toArray()));
  }

  @Test
  public void testListDirectoriesInBucket() throws Exception {
    createEmptyFile(new String[] {"foo", "bar"}, "a-list.txt");
    createEmptyFile(new String[] {"foo", "baz"}, "b-list.txt");
    createEmptyFile(new String[] {"baz"}, "c-list.txt");
    createEmptyFile(new String[] {"baz", "bar"}, "d-list.txt");
    createEmptyFile(new String[] {}, "e-list.txt");

    assertThat(s3BlobFs.listDirectories(URI.create(String.format(DIR_FORMAT, SCHEME, bucket))))
        .containsExactlyInAnyOrder("foo", "baz");
    assertThat(
            s3BlobFs.listDirectories(
                URI.create(String.format(DIR_FORMAT, SCHEME, bucket) + DELIMITER + "foo")))
        .containsExactlyInAnyOrder("bar", "baz");
    assertThat(
            s3BlobFs.listDirectories(
                URI.create(
                    String.format(DIR_FORMAT, SCHEME, bucket)
                        + DELIMITER
                        + "foo"
                        + DELIMITER
                        + "bar")))
        .isEmpty();
    assertThat(
            s3BlobFs.listDirectories(
                URI.create(String.format(DIR_FORMAT, SCHEME, bucket) + DELIMITER + "baz")))
        .containsExactlyInAnyOrder("bar");

    Throwable nonExistingPath =
        catchThrowable(
            () ->
                s3BlobFs.listDirectories(
                    URI.create(String.format(DIR_FORMAT, SCHEME, bucket) + DELIMITER + "qux")));
    assertThat(nonExistingPath).isInstanceOf(IOException.class);
    Throwable notAPath =
        catchThrowable(
            () ->
                s3BlobFs.listDirectories(
                    URI.create(
                        String.format(DIR_FORMAT, SCHEME, bucket) + DELIMITER + "e-list.txt")));
    assertThat(notAPath).isInstanceOf(IOException.class);
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
    Assert.assertEquals(actualFiles.length, originalFiles.length);

    Assert.assertTrue(Arrays.equals(actualFiles, expectedFileNames.toArray()));
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
    Assert.assertEquals(actualFiles.length, originalFiles.length);

    Assert.assertTrue(
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
    Assert.assertEquals(actualFiles.length, expectedResultList.size());
    Assert.assertTrue(Arrays.equals(expectedResultList.toArray(), actualFiles));
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
        s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(bucket, "", true));
    String[] actualResponse =
        listObjectsV2Response
            .contents()
            .stream()
            .map(x -> x.key().substring(1))
            .filter(x -> x.contains("delete"))
            .toArray(String[]::new);

    Assert.assertEquals(actualResponse.length, 2);
    Assert.assertTrue(Arrays.equals(actualResponse, expectedResultList.toArray()));
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
        s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(bucket, "", true));
    String[] actualResponse =
        listObjectsV2Response
            .contents()
            .stream()
            .map(S3Object::key)
            .filter(x -> x.contains("delete-2"))
            .toArray(String[]::new);

    Assert.assertEquals(0, actualResponse.length);
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

    Assert.assertTrue(isBucketDir);
    Assert.assertTrue(isDir);
    Assert.assertTrue(isDirChild);
    Assert.assertFalse(notIsDir);
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

    Assert.assertTrue(bucketExists);
    Assert.assertTrue(dirExists);
    Assert.assertTrue(childDirExists);
    Assert.assertTrue(fileExists);
    Assert.assertFalse(fileNotExists);
  }

  @Test
  public void testCopyFromAndToLocal() throws Exception {
    String fileName = "copyFile.txt";

    File fileToCopy = new File(getClass().getClassLoader().getResource(fileName).getFile());

    s3BlobFs.copyFromLocalFile(
        fileToCopy, URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName)));

    HeadObjectResponse headObjectResponse =
        s3Client.headObject(S3TestUtils.getHeadObjectRequest(bucket, fileName));

    Assert.assertEquals(headObjectResponse.contentLength(), (Long) fileToCopy.length());

    File fileToDownload = new File("copyFile_download.txt").getAbsoluteFile();
    s3BlobFs.copyToLocalFile(
        URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName)), fileToDownload);
    Assert.assertEquals(fileToCopy.length(), fileToDownload.length());

    fileToDownload.deleteOnExit();
  }

  @Test
  public void testOpenFile() throws Exception {
    String fileName = "sample.txt";
    String fileContent = "Hello, World";

    s3Client.putObject(
        S3TestUtils.getPutObjectRequest(bucket, fileName), RequestBody.fromString(fileContent));

    InputStream is =
        s3BlobFs.open(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName)));
    String actualContents = IOUtils.toString(is, StandardCharsets.UTF_8);
    Assert.assertEquals(actualContents, fileContent);
  }

  @Test
  public void testMkdir() throws Exception {
    String folderName = "my-test-folder";

    s3BlobFs.mkdir(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, folderName)));

    HeadObjectResponse headObjectResponse =
        s3Client.headObject(S3TestUtils.getHeadObjectRequest(bucket, folderName + DELIMITER));
    Assert.assertTrue(headObjectResponse.sdkHttpResponse().isSuccessful());
  }
}
