/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;

import com.slack.astra.s3.util.S3FileSystemInfo;
import com.slack.astra.s3.util.S3XFileSystemInfo;
import io.micrometer.core.instrument.MeterRegistry;

import java.net.URI;

public class S3XFileSystemProvider extends S3FileSystemProvider {


    static final String SCHEME = "s3x";

    /**
     * Returns the URI scheme that identifies this provider.
     *
     * @return The URI scheme (s3x)
     */
    @Override
    public String getScheme() {
        return SCHEME;
    }

    /**
     * This overrides the default AWS implementation to be able to address 3rd
     * party S3 services. To do so, we relax the default S3 URI format to the
     * following:
     * {@code
     * s3x://[accessKey:accessSecret@]endpoint/bucket/key
     * }
     *
     * Please note that the authority part of the URI (endpoint above) is always
     * considered a HTTP(S) endpoint, therefore the name of the bucket is the
     * first element of the path. The remaining path elements will be the object
     * key.
     *
     * @param uri the URI to extract the information from
     *
     * @return the information extracted from {@code uri}
     */
    @Override
    S3FileSystemInfo fileSystemInfo(URI uri) {
        return new S3XFileSystemInfo(uri);
    }

}
