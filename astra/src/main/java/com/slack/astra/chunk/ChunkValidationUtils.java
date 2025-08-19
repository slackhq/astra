package com.slack.astra.chunk;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.zip.CRC32;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NoLockFactory;

public class ChunkValidationUtils {

  public static boolean isChunkClean(Path path) throws Exception {
    FSDirectory existingDir = FSDirectory.open(path, NoLockFactory.INSTANCE);
    CheckIndex checker = new CheckIndex(existingDir);
    CheckIndex.Status status = checker.checkIndex();
    checker.close();
    return status.clean;
  }

  public static long getCRC32Checksum(String filePath) throws Exception {
    CRC32 crc = new CRC32();
    try (InputStream in = new FileInputStream(filePath)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = in.read(buffer)) != -1) {
        crc.update(buffer, 0, bytesRead);
      }
    }
    return crc.getValue();
  }
}
