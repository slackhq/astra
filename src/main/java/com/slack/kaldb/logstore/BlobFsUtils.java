package com.slack.kaldb.logstore;

import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
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

  public static void copyToS3(
      Path sourceDirPath, Collection<String> files, String bucket, String prefix, S3BlobFs blobFs)
      throws Exception {
    for (String fileName : files) {
      File fileToCopy = new File(sourceDirPath.toString(), fileName);
      // TODO: Fix the bug and remove this warning.
      // For some reason some files returned by index commit don't exist on the file system.
      // For now log a warning but this bug needs to be fixed.
      if (!fileToCopy.exists()) {
        LOG.warn("File doesn't exist at path: " + fileToCopy.getAbsolutePath());
        continue;
      }
      URI destUri =
          (prefix != null && !prefix.isEmpty())
              ? URI.create(
                  String.format(FILE_FORMAT, SCHEME, bucket + DELIMITER + prefix, fileName))
              : URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName));
      blobFs.copyFromLocalFile(fileToCopy, destUri);
    }
  }

  // TODO: Can we copy files without list files and a prefix only?
  public static String[] copyFromS3(
      String bucket, String prefix, S3BlobFs s3BlobFs, Path localDirPath) throws Exception {
    String[] s3Files =
        (prefix != null && !prefix.isEmpty())
            ? s3BlobFs.listFiles(
                URI.create(String.format(FILE_FORMAT, SCHEME, bucket + DELIMITER + prefix, "")),
                true)
            : s3BlobFs.listFiles(URI.create(String.format(FILE_FORMAT, SCHEME, bucket, "")), true);

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
