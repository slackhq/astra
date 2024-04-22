/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;

import io.reactivex.rxjava3.core.Flowable;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Iterator;

import static com.slack.astra.s3.Constants.PATH_SEPARATOR;

class S3DirectoryStream implements DirectoryStream<Path> {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final Iterator<Path> iterator;

    S3DirectoryStream(S3FileSystem fs, String bucketName, String finalDirName, Filter<? super Path> filter) {
        final var listObjectsV2Publisher = fs.client().listObjectsV2Paginator(req -> req
            .bucket(bucketName)
            .prefix(finalDirName)
            .delimiter(PATH_SEPARATOR));

        iterator = pathIteratorForPublisher(filter, fs, finalDirName, listObjectsV2Publisher);
        //noinspection ResultOfMethodCallIgnored
        iterator.hasNext();
    }

    @Override
    @NonNull
    public Iterator<Path> iterator() {
        return iterator;
    }

    @Override
    public void close() {
    }

    /**
     * Get an iterator for a {@code ListObjectsV2Publisher}
     *
     * @param filter                 a filter to apply to returned Paths. Only accepted paths will be included.
     * @param fs                     the Filesystem.
     * @param finalDirName           the directory name that will be streamed.
     * @param listObjectsV2Publisher the publisher that returns objects and common prefixes that are iterated on.
     * @return an iterator for {@code Path}s constructed from the {@code ListObjectsV2Publisher}s responses.
     * @throws SdkException          if there is an error with S3 access. This is an unchecked Exception
     */
    private Iterator<Path> pathIteratorForPublisher(
        final Filter<? super Path> filter,
        final FileSystem fs, String finalDirName,
        final ListObjectsV2Publisher listObjectsV2Publisher) throws SdkException {

        final Publisher<String> prefixPublisher =
                listObjectsV2Publisher.commonPrefixes().map(CommonPrefix::prefix);
        final Publisher<String> keysPublisher =
                listObjectsV2Publisher.contents().map(S3Object::key);

        return Flowable.concat(prefixPublisher, keysPublisher)
                .map(fs::getPath)
                .filter(path -> !isEqualToParent(finalDirName, path))  // including the parent will induce loops
                .filter(path -> tryAccept(filter, path))
                .blockingIterable()
                .iterator();
    }


    private static boolean isEqualToParent(String finalDirName, Path p) {
        return ((S3Path) p).getKey().equals(finalDirName);
    }

    private boolean tryAccept(Filter<? super Path> filter, Path path) {
        try {
            return filter.accept(path);
        } catch (IOException e) {
            logger.warn("An IOException was thrown while filtering the path: {}." +
                " Set log level to debug to show stack trace", path);
            logger.debug(e.getMessage(), e);
            return false;
        }
    }
}
