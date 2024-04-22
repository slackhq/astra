/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static com.slack.astra.s3.util.TimeOutUtils.createAndLogTimeOutMessage;

/**
 * Representation of {@link BasicFileAttributes} for an S3 object
 */
class S3BasicFileAttributes implements BasicFileAttributes {

    private static final FileTime EPOCH_FILE_TIME = FileTime.from(Instant.EPOCH);

    private static final S3BasicFileAttributes DIRECTORY_ATTRIBUTES = new S3BasicFileAttributes(
        EPOCH_FILE_TIME,
        0L,
        null,
        true,
        false
    );

    private static final Set<String> METHOD_NAMES_TO_FILTER_OUT =
        Set.of("wait", "toString", "hashCode", "getClass", "notify", "notifyAll");

    private static final Logger logger = LoggerFactory.getLogger(S3BasicFileAttributes.class.getName());


    private final FileTime lastModifiedTime;
    private final Long size;
    private final Object eTag;
    private final boolean isDirectory;
    private final boolean isRegularFile;

    /**
     * Constructor for the attributes of a path
     */
    private S3BasicFileAttributes(FileTime lastModifiedTime,
                                 Long size,
                                 Object eTag,
                                 boolean isDirectory,
                                 boolean isRegularFile
    ) {
        this.lastModifiedTime = lastModifiedTime;
        this.size = size;
        this.eTag = eTag;
        this.isDirectory = isDirectory;
        this.isRegularFile = isRegularFile;
    }

    /**
     * Returns the time of last modification.
     *
     * <p> S3 "directories" do not support a time stamp
     * to indicate the time of last modification therefore this method returns a default value
     * representing the epoch (1970-01-01T00:00:00Z) as a proxy
     *
     * @return a {@code FileTime} representing the time the file was last
     * modified.
     */
    @Override
    public FileTime lastModifiedTime() {
        return lastModifiedTime;
    }

    /**
     * Returns the time of last access.
     * <p>Without enabling S3 server access logging, CloudTrail or similar it is not possible to obtain the access time
     * of an object, therefore the current implementation will return the @{code lastModifiedTime}</p>
     *
     * @return a {@code FileTime} representing the time of last access
     */
    @Override
    public FileTime lastAccessTime() {
        return lastModifiedTime();
    }

    /**
     * Returns the creation time. The creation time is the time that the file
     * was created.
     *
     * <p> Any modification of an S3 object results in a new Object so this time will be the same as
     * {@code lastModifiedTime}. A future implementation could consider times for versioned objects.
     *
     * @return a {@code FileTime} representing the time the file was created
     */
    @Override
    public FileTime creationTime() {
        return lastModifiedTime();
    }

    /**
     * Tells whether the file is a regular file with opaque content.
     *
     * @return {@code true} if the file is a regular file with opaque content
     */
    @Override
    public boolean isRegularFile() {
        return isRegularFile;
    }

    /**
     * Tells whether the file is a directory.
     *
     * @return {@code true} if the file is a directory
     */
    @Override
    public boolean isDirectory() {
        return isDirectory;
    }

    /**
     * Tells whether the file is a symbolic link.
     *
     * @return {@code false} always as S3 has no links
     */
    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    /**
     * Tells whether the file is something other than a regular file, directory,
     * or symbolic link. There are only objects in S3 and inferred directories
     *
     * @return {@code false} always
     */
    @Override
    public boolean isOther() {
        return false;
    }

    /**
     * Returns the size of the file (in bytes). The size may differ from the
     * actual size on the file system due to compression, support for sparse
     * files, or other reasons. The size of files that are not {@link
     * #isRegularFile regular} files is implementation specific and
     * therefore unspecified.
     *
     * @return the file size, in bytes
     */
    @Override
    public long size() {
        return size;
    }

    /**
     * Returns the S3 etag for the object
     *
     * @return the etag for an object, or {@code null} for a "directory"
     * @see Files#walkFileTree
     */
    @Override
    public Object fileKey() {
        return eTag;
    }

    /**
     * Construct a <code>Map</code> representation of this object with properties filtered
     *
     * @param attributeFilter a filter to include properties in the resulting Map
     * @return a map filtered to only contain keys that pass the attributeFilter
     */
    protected Map<String, Object> asMap(Predicate<String> attributeFilter) {
        return Arrays.stream(this.getClass().getMethods())
            .filter(method -> method.getParameterCount() == 0)
            .filter(method -> !METHOD_NAMES_TO_FILTER_OUT.contains(method.getName()))
            .filter(method -> attributeFilter.test(method.getName()))
            .collect(Collectors.toMap(Method::getName, (method -> {
                logger.debug("method name: '{}'", method.getName());
                try {
                    return method.invoke(this);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    // should not ever happen as these are all public no arg methods
                    var errorMsg =
                        "an exception has occurred during a reflection operation on the methods of file attributes," +
                            "check if your Java SecurityManager is configured to allow reflection."
                        ;
                    logger.error("{}, caused by {}", errorMsg, e.getCause().getMessage());
                    throw new RuntimeException(errorMsg, e);
                }
            })));
    }

    /**
     * @param path        the path to represent the attributes of
     * @param readTimeout timeout for requests to get attributes
     * @return path BasicFileAttributes
     * @throws IOException Errors getting the metadata of the object represented by the path are wrapped in IOException
     */
    static S3BasicFileAttributes get(S3Path path, Duration readTimeout) throws IOException {
        if (path.isDirectory()) {
            return DIRECTORY_ATTRIBUTES;
        }

        var headResponse = getObjectMetadata(path, readTimeout);
        return new S3BasicFileAttributes(
            FileTime.from(headResponse.lastModified()),
            headResponse.contentLength(),
            headResponse.eTag(),
            false,
            true
        );
    }

    private static HeadObjectResponse getObjectMetadata(
        S3Path path,
        Duration timeout
    ) throws IOException {
        var client = path.getFileSystem().client();
        var bucketName = path.bucketName();
        try {
            return client.headObject(req -> req
                .bucket(bucketName)
                .key(path.getKey())
            ).get(timeout.toMillis(), MILLISECONDS);
        } catch (ExecutionException e) {
            var errMsg = format(
                "an '%s' error occurred while obtaining the metadata (for operation getFileAttributes) of '%s'" +
                    "that was not handled successfully by the S3Client's configured RetryConditions",
                e.getCause().toString(), path.toUri());
            logger.error(errMsg);
            throw new IOException(errMsg, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (TimeoutException e) {
            var msg = createAndLogTimeOutMessage(logger, "getFileAttributes", timeout.toMillis(), MILLISECONDS);
            throw new IOException(msg, e);
        }
    }

}
