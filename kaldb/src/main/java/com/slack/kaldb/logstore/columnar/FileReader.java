package com.slack.kaldb.logstore.columnar;

import java.io.IOException;

/**
 * Generic file reader interface for a particular type of Secor output file
 *
 * <p>Should be returned by a FileReaderWriterFactory that also knows how to build a corresponding
 * FileReader (that is able to read the files written by this FileWriter).
 *
 * @author Praveen Murugesan (praveen@uber.com)
 */
public interface FileReader {
  /**
   * Get the next key/value from the file
   *
   * @return KeyValue
   * @throws IOException on IO error
   */
  public KeyValue next() throws IOException;

  /**
   * Close the file
   *
   * @throws IOException on IO error
   */
  public void close() throws IOException;
}
