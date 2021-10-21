package com.slack.kaldb.logstore;

import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

/** This class contains static methods that help with blobfs operations. */
public class BlobFsUtils {

  public static final String SCHEME = "s3";
  public static final String DELIMITER = "/";
  public static final String FILE_FORMAT = "%s://%s/%s";

  public static int copyToS3(
      Path sourceDirPath, Collection<String> files, String bucket, String prefix, S3BlobFs blobFs)
      throws Exception {
    int success = 0;
    for (String fileName : files) {
      File fileToCopy = new File(sourceDirPath.toString(), fileName);
      if (!fileToCopy.exists()) {
        throw new IOException("File doesn't exist at path: " + fileToCopy.getAbsolutePath());
      }
      blobFs.copyFromLocalFile(fileToCopy, createURI(bucket, prefix, fileName));
      success++;
    }
    return success;
  }

  public static URI createURI(String bucket, String prefix, String fileName) {
    return (prefix != null && !prefix.isEmpty())
        ? URI.create(String.format(FILE_FORMAT, SCHEME, bucket + DELIMITER + prefix, fileName))
        : URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName));
  }

  // TODO: Can we copy files without list files and a prefix only?
  // TODO: Take a complete URI as this is the format stored in snapshot data
  public static String[] copyFromS3(
      String bucket, String prefix, S3BlobFs s3BlobFs, Path localDirPath) throws Exception {
    String[] s3Files = s3BlobFs.listFiles(createURI(bucket, prefix, ""), true);
    for (String fileName : s3Files) {
      URI fileToCopy = URI.create(fileName);
      File toFile =
          new File(
              localDirPath.toString(), Paths.get(fileToCopy.getPath()).getFileName().toString());
      s3BlobFs.copyToLocalFile(fileToCopy, toFile);
    }
    return s3Files;
  }

  public static void copyToLocalPath(
      Path sourceDirPath, Collection<String> files, Path destDirPath, BlobFs blobFs)
      throws IOException {
    for (String file : files) {
      blobFs.copy(
          Paths.get(sourceDirPath.toAbsolutePath().toString(), file).toUri(),
          Paths.get(destDirPath.toAbsolutePath().toString(), file).toUri());
    }
  }
}
