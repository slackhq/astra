package com.slack.kaldb.logstore.columnar;

import java.io.IOException;

/**
 * Generic file writer interface for for a particular type of Secor output file
 *
 * <p>Should be returned by a FileReaderWriterFactory that also know how to build a corresponding
 * FileReader (that is able to read the files written by this FileWriter).
 *
 * @author Praveen Murugesan (praveen@uber.com)
 */
public interface FileWriter {
  /**
   * @return length of data written up to now to the underlying file
   * @throws java.io.IOException on IO error
   */
  public long getLength() throws IOException;

  /**
   * Write the given key and value to the file
   *
   * @param keyValue KeyValue
   * @throws java.io.IOException on IO error
   */
  public void write(KeyValue keyValue) throws IOException;

  /**
   * Close the file
   *
   * @throws java.io.IOException on IO error
   */
  public void close() throws IOException;
}
