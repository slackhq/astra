/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;

import com.slack.astra.s3.util.TimeOutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class S3SeekableByteChannel implements SeekableByteChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SeekableByteChannel.class);

    private long position;
    private final S3Path path;
    private final ReadableByteChannel readDelegate;
    private final S3WritableByteChannel writeDelegate;

    private boolean closed;
    private long size = -1L;

    S3SeekableByteChannel(S3Path s3Path, S3AsyncClient s3Client, Set<? extends OpenOption> options) throws IOException {
        this(s3Path, s3Client, 0L, options, null, null);
    }

    private S3SeekableByteChannel(S3Path s3Path, S3AsyncClient s3Client, long startAt, Set<? extends OpenOption> options,
                                  Long timeout, TimeUnit timeUnit) throws IOException {
        position = startAt;
        path = s3Path;
        closed = false;
        s3Path.getFileSystem().registerOpenChannel(this);

        if (options.contains(StandardOpenOption.WRITE) && options.contains(StandardOpenOption.READ)) {
            throw new IOException("This channel does not support read and write access simultaneously");
        }
        if (options.contains(StandardOpenOption.SYNC) || options.contains(StandardOpenOption.DSYNC)) {
            throw new IOException("The SYNC/DSYNC options is not supported");
        }

        var config = s3Path.getFileSystem().configuration();
        if (options.contains(StandardOpenOption.WRITE)) {
            LOGGER.debug("using S3WritableByteChannel as write delegate for path '{}'", s3Path.toUri());
            readDelegate = null;
            var transferUtil = new S3TransferUtil(s3Client, timeout, timeUnit);
            writeDelegate = new S3WritableByteChannel(s3Path, s3Client, transferUtil, options);
            position = 0L;
        } else if (options.contains(StandardOpenOption.READ) || options.isEmpty()) {
            LOGGER.debug("using S3ReadAheadByteChannel as read delegate for path '{}'", s3Path.toUri());
            readDelegate =
                new S3ReadAheadByteChannel(s3Path, config.getMaxFragmentSize(), config.getMaxFragmentNumber(), s3Client, this,
                    timeout, timeUnit);
            writeDelegate = null;
        } else {
            throw new IOException("Invalid channel mode");
        }
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * <p> Bytes are read starting at this channel's current position, and
     * then the position is updated with the number of bytes actually read.
     * Otherwise, this method behaves exactly as specified in the {@link
     * ReadableByteChannel} interface.
     *
     * @param dst the destination buffer
     * @return the number of bytes read or -1 if no more bytes can be read.
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        validateOpen();

        if (readDelegate == null) {
            throw new NonReadableChannelException();
        }

        return readDelegate.read(dst);
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * <p> Bytes are written starting at this channel's current position, unless
     * the channel is connected to an entity such as a file that is opened with
     * the {@link StandardOpenOption#APPEND APPEND} option, in
     * which case the position is first advanced to the end. The entity to which
     * the channel is connected will grow to accommodate the
     * written bytes, and the position updates with the number of bytes
     * actually written. Otherwise, this method behaves exactly as specified by
     * the {@link WritableByteChannel} interface.
     *
     * @param src the src of the bytes to write to this channel
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        validateOpen();

        if (writeDelegate == null) {
            throw new NonWritableChannelException();
        }

        var length = src.remaining();
        this.position += length;

        return writeDelegate.write(src);
    }

    /**
     * Returns this channel's position.
     *
     * @return This channel's position,
     * a non-negative integer counting the number of bytes
     * from the beginning of the entity to the current position
     */
    @Override
    public long position() throws IOException {
        validateOpen();

        synchronized (this) {
            return position;
        }
    }

    /**
     * Sets this channel's position.
     *
     * <p> Setting the position to a value that is greater than the current size
     * is legal but does not change the size of the entity.  A later attempt to
     * read bytes at such a position will immediately return an end-of-file
     * indication.  A later attempt to write bytes at such a position will cause
     * the entity to grow to accommodate the new bytes; the values of any bytes
     * between the previous end-of-file and the newly-written bytes are
     * unspecified.
     *
     * <p> Setting the channel's position is not recommended when connected to
     * an entity, typically a file, that is opened with the {@link
     * StandardOpenOption#APPEND APPEND} option. When opened for
     * append, the position is first advanced to the end before writing.
     *
     * @param newPosition The new position, a non-negative integer counting
     *                    the number of bytes from the beginning of the entity
     * @return This channel
     * @throws ClosedChannelException   If this channel is closed
     * @throws IllegalArgumentException If the new position is negative
     * @throws IOException              If some other I/O error occurs
     */
    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (newPosition < 0) {
            throw new IllegalArgumentException("newPosition cannot be < 0");
        }

        if (!isOpen()) {
            throw new ClosedChannelException();
        }

        // this is only valid to read channels
        if (readDelegate == null) {
            throw new NonReadableChannelException();
        }

        synchronized (this) {
            position = newPosition;
            return this;
        }
    }

    /**
     * Returns the current size of entity to which this channel is connected.
     *
     * @return The current size, measured in bytes
     * @throws IOException If some other I/O error occurs
     */
    @Override
    public long size() throws IOException {
        validateOpen();

        if (size < 0) {
            fetchSize();
        }
        return this.size;
    }

    private void fetchSize() throws IOException {
        synchronized (this) {
            this.size = S3BasicFileAttributes.get(path, Duration.ofMinutes(TimeOutUtils.TIMEOUT_TIME_LENGTH_1)).size();
            LOGGER.debug("size of '{}' is '{}'", path.toUri(), this.size);
        }
    }

    /**
     * Truncates the entity, to which this channel is connected, to the given
     * size.
     *
     * <p> If the given size is less than the current size then the entity is
     * truncated, discarding any bytes beyond the new end. If the given size is
     * greater than or equal to the current size then the entity is not modified.
     * In either case, if the current position is greater than the given size
     * then it is set to that size.
     *
     * <p> An implementation of this interface may prohibit truncation when
     * connected to an entity, typically a file, opened with the {@link
     * StandardOpenOption#APPEND APPEND} option.
     *
     * @param size The new size, a non-negative byte count
     * @return This channel
     */
    @Override
    public SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException("Currently not supported");
    }

    /**
     * Tells whether this channel is open.
     *
     * @return {@code true} if, and only if, this channels delegate is open
     */
    @Override
    public boolean isOpen() {
        synchronized (this) {
            return !this.closed;
        }
    }

    /**
     * Closes this channel.
     *
     * <p> After a channel is closed, any further attempt to invoke I/O
     * operations upon it will cause a {@link ClosedChannelException} to be
     * thrown.
     *
     * <p> If this channel is already closed then invoking this method has no
     * effect.
     *
     * <p> This method may be invoked at any time.  If some other thread has
     * already invoked it, however, then another invocation will block until
     * the first invocation is complete, after which it will return without
     * effect. </p>
     */
    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (readDelegate != null) {
                readDelegate.close();
            }
            if (writeDelegate != null) {
                writeDelegate.close();
            }
            closed = true;
            path.getFileSystem().deregisterClosedChannel(this);
        }
    }

    private void validateOpen() throws ClosedChannelException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
    }

    /**
     * Access the underlying {@code ReadableByteChannel} used for reading
     *
     * @return the channel. May be null if opened for writing only
     */
    ReadableByteChannel getReadDelegate() {
        return this.readDelegate;
    }

    /**
     * Access the underlying {@code WritableByteChannel} used for writing
     *
     * @return the channel. May be null if opened for reading only
     */
    WritableByteChannel getWriteDelegate() {
        return this.writeDelegate;
    }
}
