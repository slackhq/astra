/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;

import com.slack.astra.s3.util.TimeOutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;

class S3BasicFileAttributeView implements BasicFileAttributeView {

    private final Logger logger = LoggerFactory.getLogger("S3BasicFileAttributeView");

    private final S3Path path;

    S3BasicFileAttributeView(S3Path path) {
        this.path = path;
    }

    /**
     * Returns the name of the attribute view. Attribute views of this type
     * have the name {@code "s3"}.
     */
    @Override
    public String name() {
        return "s3";
    }

    /**
     * Reads the basic file attributes as a bulk operation.
     *
     * <p> It is implementation specific if all file attributes are read as an
     * atomic operation with respect to other file system operations.
     *
     * @return the file attributes
     */
    @Override
    public BasicFileAttributes readAttributes() throws IOException {
        return S3BasicFileAttributes.get(path, Duration.ofMinutes(TimeOutUtils.TIMEOUT_TIME_LENGTH_1));
    }

    /**
     * S3 doesn't support setting of file times other than by writing the file. Therefore, this operation does
     * nothing (no-op). To support {@code Files.copy()} and {@code Files.move()} operations which call this method,
     * we don't throw an exception. The time set during those operations will be determined by S3.
     */
    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) {
        // intentional no-op. S3 doesn't support setting of file times other than by writing the file.
        logger.warn("S3 doesn't support setting of file times other than by writing the file. " +
                "The time set during those operations will be determined by S3. This method call will be ignored");
    }

}
