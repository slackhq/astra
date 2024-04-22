/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.slack.astra.s3.util.TimeOutUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * A {@code ReadableByteChannel} delegate for an {@code S3SeekableByteChannel} that maintains internal read ahead
 * buffers to reduce the amount of high latency
 * requests to S3. If the bytes required by a read are already in the buffer, they will be fulfilled from the buffer
 * rather than making another S3 request.
 * <p>As reads are made this object will update the current read position of the delegating {@code S3SeekableByteChannel}</p>
 */
class S3ReadAheadByteChannel implements ReadableByteChannel {

    private static final Logger logger = LoggerFactory.getLogger(S3ReadAheadByteChannel.class);
    private final S3AsyncClient client;
    private final S3Path path;
    private final S3SeekableByteChannel delegator;
    private final int maxFragmentSize;
    private final int maxNumberFragments;
    private final int numFragmentsInObject;
    private final long size;
    private final Long timeout;
    private final TimeUnit timeUnit;
    private boolean open;
    private final Cache<Integer, CompletableFuture<ByteBuffer>> readAheadBuffersCache;



    /**
     * Construct a new {@code S3ReadAheadByteChannel} which is used by its parent delegator to perform read operations.
     * The channel is backed by a cache that holds the buffered fragments of the object identified
     * by the {@code path}.
     *
     * @param path               the path to the S3 object being read
     * @param maxFragmentSize    the maximum amount of bytes in a read ahead fragment. Must be {@code >= 1}.
     * @param maxNumberFragments the maximum number of read ahead fragments to hold. Must be {@code >= 2}.
     * @param client             the client used to read from the {@code path}
     * @param delegator          the {@code S3SeekableByteChannel} that delegates reading to this object.
     * @param timeout            the amount of time to wait for a read ahead fragment to be available.
     * @param timeUnit           the {@code TimeUnit} for the {@code timeout}.
     * @throws IOException if a problem occurs initializing the cached fragments
     */
    S3ReadAheadByteChannel(S3Path path, int maxFragmentSize, int maxNumberFragments, S3AsyncClient client,
                           S3SeekableByteChannel delegator, Long timeout, TimeUnit timeUnit) throws IOException {
        Objects.requireNonNull(path);
        Objects.requireNonNull(client);
        Objects.requireNonNull(delegator);
        if (maxFragmentSize < 1) {
            throw new IllegalArgumentException("maxFragmentSize must be >= 1");
        }
        if (maxNumberFragments < 2) {
            throw new IllegalArgumentException("maxNumberFragments must be >= 2");
        }

        logger.debug("max read ahead fragments '{}' with size '{}' bytes", maxNumberFragments, maxFragmentSize);
        this.client = client;
        this.path = path;
        this.delegator = delegator;
        this.size = delegator.size();
        this.maxFragmentSize = maxFragmentSize;
        this.numFragmentsInObject = (int) Math.ceil((float) size / (float) maxFragmentSize);
        this.readAheadBuffersCache = Caffeine.newBuilder().maximumSize(maxNumberFragments).recordStats().build();
        this.maxNumberFragments = maxNumberFragments;
        this.open = true;
        this.timeout = timeout != null ? timeout : TimeOutUtils.TIMEOUT_TIME_LENGTH_5;
        this.timeUnit = timeUnit != null ? timeUnit : TimeUnit.MINUTES;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        Objects.requireNonNull(dst);

        var channelPosition = delegator.position();
        logger.debug("delegator position: {}", channelPosition);

        // if the position of the delegator is at the end (>= size) return -1. we're finished reading.
        if (channelPosition >= size) {
            return -1;
        }

        //figure out the index of the fragment the bytes would start in
        var fragmentIndex = fragmentIndexForByteNumber(channelPosition);
        logger.debug("fragment index: {}", fragmentIndex);

        var fragmentOffset = (int) (channelPosition - (fragmentIndex.longValue() * maxFragmentSize));
        logger.debug("fragment {} offset: {}", fragmentIndex, fragmentOffset);

        try {
            final var fragment = Objects.requireNonNull(readAheadBuffersCache.get(fragmentIndex, this::computeFragmentFuture))
                .get(timeout, timeUnit)
                .asReadOnlyBuffer();

            fragment.position(fragmentOffset);
            logger.debug("fragment remaining: {}", fragment.remaining());
            logger.debug("dst remaining: {}", dst.remaining());

            //put the bytes from fragment from the offset upto the min of fragment remaining or dst remaining
            var limit = Math.min(fragment.remaining(), dst.remaining());
            logger.debug("byte limit: {}", limit);

            var copiedBytes = new byte[limit];
            fragment.get(copiedBytes, 0, limit);
            dst.put(copiedBytes);

            if (fragment.position() >= fragment.limit() / 2) {

                // clear any fragments in cache that are lower index than this one
                clearPriorFragments(fragmentIndex);

                // until available cache slots are filled or number of fragments in file
                var maxFragmentsToLoad = Math.min(maxNumberFragments - 1, numFragmentsInObject - fragmentIndex - 1);

                for (var i = 0; i < maxFragmentsToLoad; i++) {
                    final var idxToLoad = i + fragmentIndex + 1;

                    //  add the index if it's not already there
                    if (readAheadBuffersCache.asMap().containsKey(idxToLoad)) {
                        continue;
                    }

                    logger.debug("initiate pre-loading fragment with index '{}' from '{}'", idxToLoad, path.toUri());
                    readAheadBuffersCache.put(idxToLoad, computeFragmentFuture(idxToLoad));
                }
            }

            delegator.position(channelPosition + copiedBytes.length);
            return copiedBytes.length;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // the async execution completed exceptionally.
            // not currently obvious when this will happen or if we can recover
            logger.error(
                "an exception occurred while reading bytes from {} that was not recovered by the S3 Client RetryCondition(s)",
                path.toUri());
            throw new IOException(e);
        } catch (TimeoutException e) {
            throw TimeOutUtils.logAndGenerateExceptionOnTimeOut(logger, "read",
                TimeOutUtils.TIMEOUT_TIME_LENGTH_5, TimeUnit.MINUTES);
        }
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
        readAheadBuffersCache.invalidateAll();
        readAheadBuffersCache.cleanUp();
    }

    private void clearPriorFragments(int currentFragIndx) {
        final Set<@NonNull Integer> priorIndexes = readAheadBuffersCache
            .asMap()
            .keySet().stream()
            .filter(idx -> idx < currentFragIndx)
            .collect(Collectors.toSet());

        if (!priorIndexes.isEmpty()) {
            logger.debug("invalidating fragment(s) '{}' from '{}'",
                priorIndexes.stream().map(Objects::toString).collect(Collectors.joining(", ")), path.toUri());

            readAheadBuffersCache.invalidateAll(priorIndexes);
        }
    }

    /**
     * The number of fragments currently in the cache.
     *
     * @return the size of the cache after any async evictions or reloads have happened.
     */
    int numberOfCachedFragments() {
        readAheadBuffersCache.cleanUp();
        return (int) readAheadBuffersCache.estimatedSize();
    }

    /**
     * Obtain a snapshot of the statistics of the internal cache, provides information about
     * hits, misses, requests, evictions etc. that are useful for tuning.
     *
     * @return the statistics of the internal cache.
     */
    CacheStats cacheStatistics() {
        return readAheadBuffersCache.stats();
    }

    private CompletableFuture<ByteBuffer> computeFragmentFuture(int fragmentIndex) {
        var readFrom = (long) fragmentIndex * maxFragmentSize;
        var readTo = Math.min(readFrom + maxFragmentSize, size) - 1;
        var range = "bytes=" + readFrom + "-" + readTo;
        logger.debug("byte range for {} is '{}'", path.getKey(), range);

        return client.getObject(
                builder -> builder
                    .bucket(path.bucketName())
                    .key(path.getKey())
                    .range(range),
                AsyncResponseTransformer.toBytes())
            .thenApply(BytesWrapper::asByteBuffer);
    }

    /**
     * Compute which buffer a byte should be in
     *
     * @param byteNumber the number of the byte in the object accessed by this channel
     * @return the index of the fragment in which {@code byteNumber} will be found.
     */
    Integer fragmentIndexForByteNumber(long byteNumber) {
        return Math.toIntExact(Math.floorDiv(byteNumber, (long) maxFragmentSize));
    }

    public int getMaxFragmentSize() {
        return maxFragmentSize;
    }

    public int getMaxNumberFragments() {
        return maxNumberFragments;
    }
}
