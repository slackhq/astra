package com.slack.astra.chunk;

import java.nio.file.Path;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NoLockFactory;

public class ChunkValidationUtils {

  public static boolean isChunkClean(Path path) throws Exception {
    FSDirectory existingDir = FSDirectory.open(path, NoLockFactory.INSTANCE);
    try (CheckIndex checker = new CheckIndex(existingDir)) {
      CheckIndex.Status status = checker.checkIndex();
      return status.clean;
    }
  }
}
