package com.slack.astra.blobfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class contains static methods that help with blobfs operations. */
@Deprecated
public class BlobFsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BlobFsUtils.class);

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

  // TODO: Can we copy files without list files and a prefix only?
  // TODO: Take a complete URI as this is the format stored in snapshot data
  @Deprecated
  public static String[] copyFromS3(
      String bucket, String prefix, BlobFs s3BlobFs, Path localDirPath) throws Exception {
    LOG.debug("Copying files from bucket={} prefix={} using directory", bucket, prefix);
    URI directoryToCopy = createURI(bucket, prefix, "");
    s3BlobFs.copyToLocalFile(directoryToCopy, localDirPath.toFile());
    LOG.debug("Copying S3 files complete");
    return Arrays.stream(localDirPath.toFile().listFiles())
        .map(File::toString)
        .distinct()
        .toArray(String[]::new);
  }
}
