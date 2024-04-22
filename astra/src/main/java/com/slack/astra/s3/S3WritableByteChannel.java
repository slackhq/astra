/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;

import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

class S3WritableByteChannel implements WritableByteChannel {
    private final S3Path path;
    private final Path tempFile;
    private final SeekableByteChannel channel;
    private final S3TransferUtil s3TransferUtil;

    private boolean open;

    S3WritableByteChannel(
        S3Path path,
        S3AsyncClient client,
        S3TransferUtil s3TransferUtil,
        Set<? extends OpenOption> options
    ) throws IOException {
        Objects.requireNonNull(path);
        Objects.requireNonNull(client);
        this.s3TransferUtil = s3TransferUtil;
        this.path = path;

        try {
            var fileSystemProvider = (S3FileSystemProvider) path.getFileSystem().provider();
            var exists = fileSystemProvider.exists(client, path);

            if (exists && options.contains(StandardOpenOption.CREATE_NEW)) {
                throw new FileAlreadyExistsException("File at path:" + path + " already exists");
            }
            if (!exists && !options.contains(StandardOpenOption.CREATE_NEW) && !options.contains(StandardOpenOption.CREATE)) {
                throw new NoSuchFileException("File at path:" + path + " does not exist yet");
            }

            tempFile = Files.createTempFile("aws-s3-nio-", ".tmp");
            if (exists) {
                s3TransferUtil.downloadToLocalFile(path, tempFile);
            }

            channel = Files.newByteChannel(this.tempFile, removeCreateNew(options));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not open the path:" + path, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new IOException("Could not open the path:" + path, e);
        }
        this.open = true;
    }

    private @NonNull Set<? extends OpenOption> removeCreateNew(Set<? extends OpenOption> options) {
        var auxOptions = new HashSet<>(options);
        auxOptions.remove(StandardOpenOption.CREATE_NEW);
        return Set.copyOf(auxOptions);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return channel.write(src);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        channel.close();

        s3TransferUtil.uploadLocalFile(path, tempFile);
        Files.deleteIfExists(tempFile);

        open = false;
    }
}
