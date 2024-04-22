/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3.util;

import software.amazon.awssdk.services.s3.internal.BucketUtils;

import java.net.URI;

/**
 * Populates fields with information extracted by the S3 URI provided. This
 * implementation is for standard AWS buckets as described in section
 * "Accessing a bucket using S3://" in https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html
 * <p>
 * It also computes the file system key that can be used to identify a runtime
 * instance of a S3FileSystem (for caching purposes for example). In this
 * implementation the key is the bucket name (which is unique in the AWS S3
 * namespace).
 */
public class S3XFileSystemInfo extends S3FileSystemInfo {

    private static final String URI_PATH_SEPARATOR = "/";

    /**
     * Creates a new instance and populates it with the information extracted
     * from {@code uri}. The provided uri is parsed accordingly to the following
     * format:
     * <p>
     * {@code
     * <p>
     * s3x://[accessKey:accessSecret@]endpoint[:port]/bucket/key
     * <p>
     * }
     * <p>
     * Please note that the authority part of the URI (endpoint[:port] above) is always
     * considered a HTTP(S) endpoint, therefore the name of the bucket is the
     * first element of the path. The remaining path elements will be the object
     * key.
     * <p>
     * Additionally {@code key} is computed as endpoint/bucket/accessKey
     *
     * @param uri a S3 URI
     * @throws IllegalArgumentException if URI contains invalid components
     *                                  (e.g. an invalid bucket name)
     */
    public S3XFileSystemInfo(URI uri) throws IllegalArgumentException {
        if (uri == null) {
            throw new IllegalArgumentException("uri can not be null");
        }

        final var userInfo = uri.getUserInfo();

        if (userInfo != null) {
            var pos = userInfo.indexOf(':');
            accessKey = (pos < 0) ? userInfo : userInfo.substring(0, pos);
            accessSecret = (pos < 0) ? null : userInfo.substring(pos + 1);
        }

        endpoint = uri.getHost();
        if (uri.getPort() > 0) {
            endpoint += ":" + uri.getPort();
        }
        bucket = uri.getPath().split(URI_PATH_SEPARATOR)[1];

        BucketUtils.isValidDnsBucketName(bucket, true);

        key = endpoint + '/' + bucket;
        if (accessKey != null) {
            key = accessKey + '@' + key;
        }
    }
}
