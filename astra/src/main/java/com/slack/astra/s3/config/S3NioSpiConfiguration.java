/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.s3.internal.BucketUtils;
import software.amazon.awssdk.utils.Pair;

import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Object to hold configuration of the S3 NIO SPI
 */
public class S3NioSpiConfiguration extends HashMap<String, Object> {

    public static final String AWS_REGION_PROPERTY = "aws.region";
    public static final String AWS_ACCESS_KEY_PROPERTY = "aws.accessKey";
    public static final String AWS_SECRET_ACCESS_KEY_PROPERTY = "aws.secretAccessKey";

    /**
     * The name of the maximum fragment size property
     */
    public static final String S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY = "s3.spi.read.max-fragment-size";
    /**
     * The default value of the maximum fragment size property
     */
    public static final int S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT = 5242880;
    /**
     * The name of the maximum fragment number property
     */
    public static final String S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY = "s3.spi.read.max-fragment-number";
    /**
     * The default value of the maximum fragment size property
     */
    public static final int S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT = 50;
    /**
     * The name of the endpoint property
     */
    public static final String S3_SPI_ENDPOINT_PROPERTY = "s3.spi.endpoint";
    /**
     * The default value of the endpoint property
     */
    public static final String S3_SPI_ENDPOINT_DEFAULT = "";
    /**
     * The name of the endpoint protocol property
     */
    public static final String S3_SPI_ENDPOINT_PROTOCOL_PROPERTY = "s3.spi.endpoint-protocol";
    /**
     * The default value of the endpoint protocol property
     */
    public static final String S3_SPI_ENDPOINT_PROTOCOL_DEFAULT = "https";
    /**
     * The name of the force path style property
     */
    public static final String S3_SPI_FORCE_PATH_STYLE_PROPERTY = "s3.spi.force-path-style";
    /**
     * The default value of the credentials property
     */
    public static final String S3_SPI_CREDENTIALS_PROPERTY = "s3.spi.credentials";

    private static final Pattern ENDPOINT_REGEXP = Pattern.compile("(\\w[\\w\\-\\.]*)?(:(\\d+))?");

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String bucketName;


    /**
     * Create a new, empty configuration
     */
    public S3NioSpiConfiguration() {
        this(new HashMap<>());
    }

    /**
     * Create a new, empty configuration
     *
     * @param overrides configuration to override default values
     */
    public S3NioSpiConfiguration(Map<String, ?> overrides) {
        Objects.requireNonNull(overrides);

        //
        // setup defaults
        //
        put(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, String.valueOf(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT));
        put(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, String.valueOf(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT));
        put(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, S3_SPI_ENDPOINT_PROTOCOL_DEFAULT);

        //
        // With the below we pick existing environment variables and system
        // properties as overrides of the default aws-nio specific properties.
        // We do not pick aws generic properties like aws.region or
        // aws.accessKey, leaving the framework and the underlying AWS client
        // the possibility to use the standard behaviour.
        //

        //add env var overrides if present
        keySet().stream()
            .map(key -> Pair.of(key,
                Optional.ofNullable(System.getenv().get(this.convertPropertyNameToEnvVar(key)))))
            .forEach(pair -> pair.right().ifPresent(val -> put(pair.left(), val)));

        //add System props as overrides if present
        keySet().forEach(
            key -> Optional.ofNullable(System.getProperty(key)).ifPresent(val -> put(key, val))
        );

        overrides.keySet().forEach(key -> put(key, overrides.get(key)));
    }

    /**
     * Create a new configuration with overrides
     *
     * @param overrides the overrides
     */
    protected S3NioSpiConfiguration(Properties overrides) {
        Objects.requireNonNull(overrides);
        overrides.stringPropertyNames()
            .forEach(key -> put(key, overrides.getProperty(key)));
    }

    /**
     * Fluently sets the value of maximum fragment number
     *
     * @param maxFragmentNumber the maximum fragment number
     * @return this instance
     */
    public S3NioSpiConfiguration withMaxFragmentNumber(int maxFragmentNumber) {
        if (maxFragmentNumber < 1) {
            throw new IllegalArgumentException("maxFragmentNumber must be positive");
        }
        put(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, String.valueOf(maxFragmentNumber));
        return this;
    }

    /**
     * Fluently sets the value of maximum fragment size
     *
     * @param maxFragmentSize the maximum fragment size
     * @return this instance
     */
    public S3NioSpiConfiguration withMaxFragmentSize(int maxFragmentSize) {
        if (maxFragmentSize < 1) {
            throw new IllegalArgumentException("maxFragmentSize must be positive");
        }
        put(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, String.valueOf(maxFragmentSize));
        return this;
    }

    /**
     * Fluently sets the value of the endpoint
     *
     * @param endpoint the endpoint
     * @return this instance
     */
    public S3NioSpiConfiguration withEndpoint(String endpoint) {

        if (endpoint == null) {
            endpoint = "";
        }
        endpoint = endpoint.trim();

        if (!endpoint.isEmpty() && !ENDPOINT_REGEXP.matcher(endpoint).matches()) {
            throw new IllegalArgumentException(
                String.format("endpoint '%s' does not match format host:port where port is a number", endpoint)
            );
        }

        put(S3_SPI_ENDPOINT_PROPERTY, endpoint);
        return this;
    }

    /**
     * Fluently sets the value of the endpoint's protocol
     *
     * @param protocol the endpoint's protcol
     * @return this instance
     */
    public S3NioSpiConfiguration withEndpointProtocol(String protocol) {
        if (protocol != null) {
            protocol = protocol.trim();
        }
        if (!"http".equals(protocol) && !"https".equals(protocol)) {
            throw new IllegalArgumentException("endpoint prococol must be one of ('http', 'https')");
        }
        put(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, protocol);
        return this;
    }

    /**
     * Fluently sets the value of the region
     *
     * @param region the region; if null or blank the property is removed
     * @return this instance
     */
    public S3NioSpiConfiguration withRegion(String region) {
        if ((region == null) || region.isBlank()) {
            remove(AWS_REGION_PROPERTY);
        } else {
            put(AWS_REGION_PROPERTY, region.trim());
        }

        return this;
    }

    /**
     * Fluently sets the value of bucketName
     *
     * @param bucketName the bucket name
     * @return this instance
     * @throws IllegalArgumentException if bucketName is not compliant with
     *                                  https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
     */
    public S3NioSpiConfiguration withBucketName(String bucketName) {
        if (bucketName != null) {
            BucketUtils.isValidDnsBucketName(bucketName, true);
        }
        this.bucketName = bucketName;
        return this;
    }

    /**
     * Fluently sets the value of accessKey and secretAccessKey
     *
     * @param accessKey       the accesskey; if null, credentials are removed
     * @param secretAccessKey the secretAccesskey; if accessKey is not null, it can not be null
     * @return this instance
     */
    public S3NioSpiConfiguration withCredentials(String accessKey, String secretAccessKey) {
        AwsCredentials credentials = null;
        if (accessKey == null) {
            remove(AWS_ACCESS_KEY_PROPERTY);
            remove(AWS_SECRET_ACCESS_KEY_PROPERTY);
        } else {
            if (secretAccessKey == null) {
                throw new IllegalArgumentException("secretAccessKey can not be null");
            }
            put(AWS_ACCESS_KEY_PROPERTY, accessKey);
            put(AWS_SECRET_ACCESS_KEY_PROPERTY, secretAccessKey);
            credentials = AwsBasicCredentials.create(accessKey, secretAccessKey);
        }
        withCredentials(credentials);

        return this;
    }

    /**
     * Fluently sets the value of accessKey and secretAccessKey given a
     * {@code AwsCredentials} object.
     *
     * @param credentials the credentials; if null, credentials are removed
     * @return this instance
     */
    public S3NioSpiConfiguration withCredentials(AwsCredentials credentials) {
        if (credentials == null) {
            remove(S3_SPI_CREDENTIALS_PROPERTY);
        } else {
            put(S3_SPI_CREDENTIALS_PROPERTY, credentials);
        }
        return this;
    }

    /**
     * Fluently sets the value of {@code forcePathStyle} and adds
     * {@code S3_SPI_FORCE_PATH_STYLE_PROPERTY} to the map unless the given
     * value is null. If null, {@code S3_SPI_FORCE_PATH_STYLE_PROPERTY} is
     * removed from the map.
     *
     * @param forcePathStyle the new value; can be null
     * @return this instance
     */
    public S3NioSpiConfiguration withForcePathStyle(Boolean forcePathStyle) {
        if (forcePathStyle == null) {
            remove(S3_SPI_FORCE_PATH_STYLE_PROPERTY);
        } else {
            put(S3_SPI_FORCE_PATH_STYLE_PROPERTY, forcePathStyle);
        }

        return this;
    }

    /**
     * Get the value of the Maximum Fragment Size
     *
     * @return the configured value or the default if not overridden
     */
    public int getMaxFragmentSize() {
        return parseIntProperty(
            S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY,
            S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT
        );
    }

    /**
     * Get the value of the Maximum Fragment Number
     *
     * @return the configured value or the default if not overridden
     */
    public int getMaxFragmentNumber() {
        return parseIntProperty(
            S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY,
            S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT
        );
    }

    /**
     * Get the value of the endpoint. Not that no endvar/sysprop is taken as
     * default.
     *
     * @return the configured value or the default ("") if not overridden
     */
    public String getEndpoint() {
        return (String) getOrDefault(S3_SPI_ENDPOINT_PROPERTY, S3_SPI_ENDPOINT_DEFAULT);
    }

    /**
     * Get the value of the endpoint protocol
     *
     * @return the configured value or the default if not overridden
     */
    public String getEndpointProtocol() {
        var protocol = (String) getOrDefault(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, S3_SPI_ENDPOINT_PROTOCOL_DEFAULT);
        if ("http".equalsIgnoreCase(protocol) || "https".equalsIgnoreCase(protocol)) {
            return protocol;
        }
        logger.warn("the value of '{}' for '{}' is not 'http'|'https', using default value of '{}'",
            protocol, S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, S3_SPI_ENDPOINT_PROTOCOL_DEFAULT);
        return S3_SPI_ENDPOINT_PROTOCOL_DEFAULT;
    }

    /**
     * Get the configured credentials. Note that credentials can be provided in
     * two ways:
     * <p>
     * 1. {@code withCredentials(String accessKey, String secretAcccessKey)}
     * 2. {@code withCredentials(AwsCredentials credentials)}
     * <p>
     * The latter takes the priority, so if both are used, {@code getCredentials()}
     * returns the most complete object, which is the value of the property
     * {@code S3_SPI_CREDENTIALS_PROPERTY}
     *
     * @return the configured value or null if not provided
     */
    public AwsCredentials getCredentials() {
        if (containsKey(S3_SPI_CREDENTIALS_PROPERTY)) {
            return (AwsCredentials) get(S3_SPI_CREDENTIALS_PROPERTY);
        }
        if (containsKey(AWS_ACCESS_KEY_PROPERTY)) {
            return AwsBasicCredentials.create(
                (String) get(AWS_ACCESS_KEY_PROPERTY),
                (String) get(AWS_SECRET_ACCESS_KEY_PROPERTY)
            );
        }

        return null;
    }

    /**
     * Get the configured region if any
     *
     * @return the configured value or null if not provided
     */
    public String getRegion() {
        return (String) get(AWS_REGION_PROPERTY);
    }

    /**
     * Get the configured bucket name if configured
     *
     * @return the configured value or null if not provided
     */
    public String getBucketName() {
        return bucketName;
    }

    public boolean getForcePathStyle() {
        return (boolean) getOrDefault(S3_SPI_FORCE_PATH_STYLE_PROPERTY, false);
    }

    /**
     * Generates an environment variable name from a property name. E.g 'some.property' becomes 'SOME_PROPERTY'
     *
     * @param propertyName the name to convert
     * @return the converted name
     */
    protected String convertPropertyNameToEnvVar(String propertyName) {
        if (propertyName == null || propertyName.trim().isEmpty()) {
            return "";
        }

        return propertyName
            .trim()
            .replace('.', '_').replace('-', '_')
            .toUpperCase(Locale.ROOT);
    }

    private int parseIntProperty(String propName, int defaultVal) {
        var propertyVal = (String) get(propName);
        try {
            return Integer.parseInt(propertyVal);
        } catch (NumberFormatException e) {
            logger.warn("the value of '{}' for '{}' is not an integer, using default value of '{}'",
                propertyVal, propName, defaultVal);
            return defaultVal;
        }
    }

    public URI endpointUri() {
        var endpoint = getEndpoint();
        if (endpoint.isBlank()) {
            return null;
        }
        return URI.create(String.format("%s://%s", getEndpointProtocol(), getEndpoint()));
    }
}
