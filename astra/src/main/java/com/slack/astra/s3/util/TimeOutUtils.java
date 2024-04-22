/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3.util;

import org.slf4j.Logger;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class TimeOutUtils {

    public static final long TIMEOUT_TIME_LENGTH_1 = 1L;
    public static long TIMEOUT_TIME_LENGTH_3 = 3L;
    public static final long TIMEOUT_TIME_LENGTH_5 = 5L;

    private TimeOutUtils() {
        
    }

    /**
     * Generate a time-out message string.
     *
     * @param operationName the name of the operation that timed out
     * @param length        the length of time
     * @param unit          the unit of time
     * @return the message
     */
    public static String createTimeOutMessage(String operationName, long length, TimeUnit unit) {
        return format("the %s operation timed out after %d %s, check your network connectivity and status of S3 service",
            operationName, length, unit.toString().toLowerCase(Locale.ROOT));
    }

    /**
     * Creates a time-out message and logs the same to the <code>logger</code>
     *
     * @param logger        to which the message is logged
     * @param operationName the name of the operation that timed out
     * @param length        the length of time
     * @param unit          the unit of time
     * @return the message logged
     */
    public static String createAndLogTimeOutMessage(Logger logger, String operationName, long length, TimeUnit unit) {
        Objects.requireNonNull(logger);
        var msg = createTimeOutMessage(operationName, length, unit);
        logger.error(msg);
        return msg;
    }

    /**
     * Generate a RuntimeException representing the time-out and log the message containerd in the exception
     *
     * @param logger        to which the message is logged
     * @param operationName the name of the operation that timed out
     * @param length        the length of time
     * @param unit          the unit of time
     * @return the exception generated, ready to throw.
     */
    public static RuntimeException logAndGenerateExceptionOnTimeOut(Logger logger, String operationName, long length,
                                                                    TimeUnit unit) {
        Objects.requireNonNull(logger);
        var msg = createAndLogTimeOutMessage(logger, operationName, length, unit);
        return new RuntimeException(msg);
    }
}
