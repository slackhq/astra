package com.slack.kaldb.logstore.columnar.util;

import com.slack.kaldb.logstore.columnar.FileReader;
import com.slack.kaldb.logstore.columnar.FileReaderWriterFactory;
import com.slack.kaldb.logstore.columnar.FileWriter;
import com.slack.kaldb.logstore.columnar.common.LogFilePath;
import com.slack.kaldb.logstore.columnar.common.SecorConfig;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * ReflectionUtil implements utility methods to construct objects of classes specified by name.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 * @author Silas Davis (github-code@silasdavis.net)
 */
public class ReflectionUtil {
  /**
   * Create a FileReaderWriterFactory that is able to read and write a specific type of output log
   * file. The class passed in by name must be assignable to FileReaderWriterFactory. Allows for
   * pluggable FileReader and FileWriter instances to be constructed for a particular type of log
   * file.
   *
   * <p>See the secor.file.reader.writer.factory config option.
   *
   * @param className the class name of a subclass of FileReaderWriterFactory
   * @param config The SecorCondig to initialize the FileReaderWriterFactory with
   * @return a FileReaderWriterFactory with the runtime type of the class passed by name
   * @throws Exception
   */
  private static FileReaderWriterFactory createFileReaderWriterFactory(
      String className, SecorConfig config) throws Exception {
    Class<?> clazz = Class.forName(className);
    if (!FileReaderWriterFactory.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException(
          String.format(
              "The class '%s' is not assignable to '%s'.",
              className, FileReaderWriterFactory.class.getName()));
    }

    try {
      // Try to load constructor that accepts single parameter - secor
      // configuration instance
      return (FileReaderWriterFactory) clazz.getConstructor(SecorConfig.class).newInstance(config);
    } catch (NoSuchMethodException e) {
      // Fallback to parameterless constructor
      return (FileReaderWriterFactory) clazz.newInstance();
    }
  }

  /**
   * Use the FileReaderWriterFactory specified by className to build a FileWriter
   *
   * @param className the class name of a subclass of FileReaderWriterFactory to create a FileWriter
   *     from
   * @param logFilePath the LogFilePath that the returned FileWriter should write to
   * @param codec an instance CompressionCodec to compress the file written with, or null for no
   *     compression
   * @param config The SecorCondig to initialize the FileWriter with
   * @return a FileWriter specialised to write the type of files supported by the
   *     FileReaderWriterFactory
   * @throws Exception on error
   */
  public static FileWriter createFileWriter(
      String className, LogFilePath logFilePath, CompressionCodec codec, SecorConfig config)
      throws Exception {
    return createFileReaderWriterFactory(className, config).BuildFileWriter(logFilePath, codec);
  }

  /**
   * Use the FileReaderWriterFactory specified by className to build a FileReader
   *
   * @param className the class name of a subclass of FileReaderWriterFactory to create a FileReader
   *     from
   * @param logFilePath the LogFilePath that the returned FileReader should read from
   * @param codec an instance CompressionCodec to decompress the file being read, or null for no
   *     compression
   * @param config The SecorCondig to initialize the FileReader with
   * @return a FileReader specialised to read the type of files supported by the
   *     FileReaderWriterFactory
   * @throws Exception on error
   */
  public static FileReader createFileReader(
      String className, LogFilePath logFilePath, CompressionCodec codec, SecorConfig config)
      throws Exception {
    return createFileReaderWriterFactory(className, config).BuildFileReader(logFilePath, codec);
  }
}
