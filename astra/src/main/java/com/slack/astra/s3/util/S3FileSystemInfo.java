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
 * "Accessing a bucket using S3://"
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html">here</a>
 * <br>
 * It also computes the file system key that can be used to identify a runtime
 * instance of a S3FileSystem (for caching purposes for example). In this
 * implementation the key is the bucket name (which is unique in the AWS S3
 * namespace).
 */
public class S3FileSystemInfo {

    protected String key;
    protected String endpoint;
    protected String bucket;
    protected String accessKey;
    protected String accessSecret;

    protected S3FileSystemInfo() {
    }

    /**
     * Creates a new instance and populates it with key and bucket. The name of
     * the bucket must follow AWS S3
     * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html">bucket naming rules</a>)
     *
     * @param uri a S3 URI
     * @throws IllegalArgumentException if URI contains invalid components
     *                                  (e.g. an invalid bucket name)
     */
    public S3FileSystemInfo(URI uri) throws IllegalArgumentException {
        if (uri == null) {
            throw new IllegalArgumentException("uri can not be null");
        }

        key = uri.getAuthority();
        bucket = uri.getAuthority();

        BucketUtils.isValidDnsBucketName(bucket, true);
    }

    public String key() {
        return key;
    }

    public String endpoint() {
        return endpoint;
    }

    public String bucket() {
        return bucket;
    }

    public String accessKey() {
        return accessKey;
    }

    public String accessSecret() {
        return accessSecret;
    }
}
