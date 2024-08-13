package com.slack.astra.blobfs;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;

/**
 * BlobFs is a restricted FS API that exposes functionality that is required for a store to use
 * different FS implementations. The restrictions are in place due to 2 driving factors: 1. Prevent
 * unexpected performance hit when a broader API is implemented - especially, we would like to
 * reduce calls to remote filesystems that might be needed for a broader API, but not necessarily
 * required by blobfs lib(see the documentation for move() method below). 2. Provide an interface
 * that is simple to be implemented across different FS types. The contract that developers have to
 * adhere to will be simpler. Please read the method level docs carefully to note the exceptions
 * while using the APIs.
 *
 * <p>NOTE: This code is a fork of PinotFS from Apache Pinot. In future, we will import this code as
 * an external lib.
 */
@Deprecated
public abstract class BlobFs implements Closeable, Serializable {

  /**
   * Deletes the file at the location provided. If the segmentUri is a directory, it will delete the
   * entire directory.
   *
   * @param segmentUri URI of the segment
   * @param forceDelete true if we want the uri and any sub-uris to always be deleted, false if we
   *     want delete to fail when we want to delete a directory and that directory is not empty
   * @return true if delete is successful else false
   * @throws IOException on IO failure, e.g Uri is not present or not valid
   */
  @Deprecated
  public abstract boolean delete(URI segmentUri, boolean forceDelete) throws IOException;

  /**
   * Checks whether the file or directory at the provided location exists.
   *
   * @param fileUri URI of file
   * @return true if path exists
   * @throws IOException on IO failure
   */
  @Deprecated
  public abstract boolean exists(URI fileUri) throws IOException;

  /**
   * Lists all the files and directories at the location provided. Lists recursively if {@code
   * recursive} is set to true. Throws IOException if this abstract pathname is not valid, or if an
   * I/O error occurs.
   *
   * @param fileUri location of file
   * @param recursive if we want to list files recursively
   * @return an array of strings that contains file paths
   * @throws IOException on IO failure. See specific implementation
   */
  @Deprecated
  public abstract String[] listFiles(URI fileUri, boolean recursive) throws IOException;

  /**
   * Copies a file from a remote filesystem to the local one. Keeps the original file.
   *
   * @param srcUri location of current file on remote filesystem
   * @param dstFile location of destination on local filesystem
   * @throws Exception if srcUri is not valid or not present, or timeout when downloading file to
   *     local
   */
  @Deprecated
  public abstract void copyToLocalFile(URI srcUri, File dstFile) throws Exception;

  /**
   * The src file is on the local disk. Add it to filesystem at the given dst name and the source is
   * kept intact afterwards.
   *
   * @param srcFile location of src file on local disk
   * @param dstUri location of dst on remote filesystem
   * @throws Exception if fileUri is not valid or not present, or timeout when uploading file from
   *     local
   */
  @Deprecated
  public abstract void copyFromLocalFile(File srcFile, URI dstUri) throws Exception;

  /**
   * Allows us the ability to determine whether the uri is a directory.
   *
   * @param uri location of file or directory
   * @return true if uri is a directory, false otherwise.
   * @throws IOException on IO failure, e.g uri is not valid or not present
   */
  @Deprecated
  public abstract boolean isDirectory(URI uri) throws IOException;

  /**
   * For certain filesystems, we may need to close the filesystem and do relevant operations to
   * prevent leaks. By default, this method does nothing.
   *
   * @throws IOException on IO failure
   */
  @Override
  @Deprecated
  public void close() throws IOException {}
}
