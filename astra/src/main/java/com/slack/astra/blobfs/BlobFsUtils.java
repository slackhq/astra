package com.slack.astra.blobfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collection;

/** This class contains static methods that help with blobfs operations. */
@Deprecated
public class BlobFsUtils {

  public static final String SCHEME = "s3";
  public static final String DELIMITER = "/";
  public static final String FILE_FORMAT = "%s://%s/%s";

  @Deprecated
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

  @Deprecated
  public static URI createURI(String bucket, String prefix, String fileName) {
    return (prefix != null && !prefix.isEmpty())
        ? URI.create(String.format(FILE_FORMAT, SCHEME, bucket + DELIMITER + prefix, fileName))
        : URI.create(String.format(FILE_FORMAT, SCHEME, bucket, fileName));
  }
}
