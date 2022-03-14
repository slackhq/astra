package com.slack.kaldb.logstore;

import com.slack.kaldb.blobfs.BlobFs;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class contains static methods that help with blobfs operations. */
public class BlobFsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BlobFsUtils.class);

  public static final String SCHEME = "s3";
  public static final String DELIMITER = "/";
  public static final String FILE_FORMAT = "%s://%s/%s";

  public static int copyToS3(
      Path sourceDirPath, Collection<String> files, String bucket, String prefix, BlobFs blobFs)
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
      String bucket, String prefix, BlobFs s3BlobFs, Path localDirPath) throws Exception {
    String[] s3Files = s3BlobFs.listFiles(createURI(bucket, prefix, ""), true);
    LOG.info(
        "Copying files from bucket={} prefix={} filesToCopy={}", bucket, prefix, s3Files.length);
    for (String fileName : s3Files) {
      URI fileToCopy = URI.create(fileName);
      File toFile =
          new File(
              localDirPath.toString(), Paths.get(fileToCopy.getPath()).getFileName().toString());
      s3BlobFs.copyToLocalFile(fileToCopy, toFile);
    }
    LOG.info("Copying S3 files complete");
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
