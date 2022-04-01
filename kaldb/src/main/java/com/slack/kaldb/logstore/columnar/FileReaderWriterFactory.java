package com.slack.kaldb.logstore.columnar;

import com.slack.kaldb.logstore.columnar.common.LogFilePath;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * Provides a single factory class to make FileReader and FileWriter instances that can read from
 * and write to the same type of output file.
 *
 * <p>Implementers of this interface should provide a zero-argument constructor so that they can be
 * constructed generically when referenced in configuration; see ReflectionUtil for details.
 *
 * @author Silas Davis (github-code@silasdavis.net)
 */
public interface FileReaderWriterFactory {
  /**
   * Build a FileReader instance to read from the target log file
   *
   * @param logFilePath the log file to read from
   * @param codec the compression codec the file was written with (use null for no codec, or to
   *     auto-detect from file headers where supported)
   * @return a FileReader instance to read from the target log file
   * @throws IllegalAccessException on illegal access
   * @throws Exception on error
   * @throws InstantiationException on instantiation error
   */
  public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec)
      throws Exception;
  /**
   * Build a FileWriter instance to write to the target log file
   *
   * @param logFilePath the log file to read from
   * @param codec the compression codec to write the file with
   * @return a FileWriter instance to write to the target log file
   * @throws Exception on error
   */
  public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec)
      throws Exception;
}
