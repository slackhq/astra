/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;

import com.slack.astra.s3.config.S3NioSpiConfiguration;
import com.slack.astra.s3.util.S3FileSystemInfo;
import com.slack.astra.s3.util.TimeOutUtils;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static com.slack.astra.s3.Constants.PATH_SEPARATOR;
import static com.slack.astra.s3.util.TimeOutUtils.TIMEOUT_TIME_LENGTH_1;
import static com.slack.astra.s3.util.TimeOutUtils.logAndGenerateExceptionOnTimeOut;

/**
 * Service-provider class for S3 when represented as an NIO filesystem. The methods defined by the Files class will
 * delegate to an instance of this class when referring to an object in S3. This class will in turn make calls to the
 * S3 service.
 * <br>
 * This class should never be used directly. It is invoked by the service loader when, for example, the java.nio.file.Files
 * class is used to address an object beginning with the scheme "s3".
 */
public class S3FileSystemProvider extends FileSystemProvider {

    private final MeterRegistry meterRegistry;

    public S3FileSystemProvider(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Constant for the S3 scheme "s3"
     */
    static final String SCHEME = "s3";
    private static final Map<String, S3FileSystem> FS_CACHE = new ConcurrentHashMap<>();

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());


    /**
     * Returns the URI scheme that identifies this provider.
     *
     * @return The URI scheme (s3)
     */
    @Override
    public String getScheme() {
        return SCHEME;
    }

    /**
     *
     * <em><b>Experimental</b></em>. Attempts to create a new S3 bucket based on the "authority" part of the URI and returns the
     * {@code FileSystem} object identified by the URI.
     *
     * @param uri    The URI to identify the file system
     * @param env    The environment to be used when creating the file system. May be null or empty.
     *               The following keys are supported:
     *               <ul>
     *               <li>acl</li>
     *               <li>grantFullControl</li>
     *               <li>grantRead</li>
     *               <li>grantReadACP</li>
     *               <li>grantWrite</li>
     *               <li>grantWriteACP</li>
     *               <li>locationConstraint</li>
     *               </ul>
     *               The values should be @code{String}s or may be objects if the @code{toString()} method of those objects
     *               produce @code{String}s that would be accepted by the associated S3 create bucket builders. All other
     *               keys are currently ignored but future implementations may support additional keys and may also throw
     *               an @link{IllegalArgumentException} if they are not recognized.
     * @return       The new file system
     * @since       2.0.0, the current implementation is experimental and may change in the future.
     * @throws IOException If an exception occurs. In all cases the exception will wrap a causal exception which could be
     *                     an SDKException thrown by the underlying S3 service or may be one of:
     *                     ExecutionException, InterruptedException, or TimeoutException if a problem occurs with the
     *                     asynchronous call to the service.
     * @throws IllegalArgumentException if the URI scheme is not "s3".
     */
    @Override
    public FileSystem newFileSystem(final URI uri, final Map<String, ?> env) throws IOException {
        if (!uri.getScheme().equals(getScheme())) {
            throw new IllegalArgumentException("URI scheme must be " + getScheme());
        }

        @SuppressWarnings("unchecked")
        var envMap = (Map<String, Object>) env;

        var info = fileSystemInfo(uri);
        var config = new S3NioSpiConfiguration().withEndpoint(info.endpoint()).withBucketName(info.bucket());
        if (info.accessKey() != null) {
            config.withCredentials(info.accessKey(), info.accessSecret());
        }
        var bucketName = config.getBucketName();

        try (var client = new S3ClientProvider(config).configureCrtClient().build()) {
            var createBucketResponse = client.createBucket(
                    bucketBuilder -> bucketBuilder.bucket(bucketName)
                            .acl(envMap.getOrDefault("acl", "").toString())
                            .grantFullControl(envMap.getOrDefault("grantFullControl", "").toString())
                            .grantRead(envMap.getOrDefault("grantRead", "").toString())
                            .grantReadACP(envMap.getOrDefault("grantReadACP", "").toString())
                            .grantWrite(envMap.getOrDefault("grantWrite", "").toString())
                            .grantWriteACP(envMap.getOrDefault("grantWriteACP", "").toString())
                            .createBucketConfiguration(confBuilder -> {
                                if (envMap.containsKey("locationConstraint")) {
                                    String loc = envMap.get("locationConstraint").toString();
                                    if (loc.equals(Region.US_EAST_1.id())) {
                                        loc = null; // us-east-1 is the default (null) location for S3
                                    }
                                    confBuilder.locationConstraint(loc);
                                }
                            })
            ).get(30, TimeUnit.SECONDS);
            logger.debug("Create bucket response {}", createBucketResponse.toString());

        } catch (ExecutionException e) {
            throw new IOException(e.getMessage(), e.getCause());
        } catch (InterruptedException | TimeoutException | SdkException e) {
            throw new IOException(e.getMessage(), e);
        }
        return getFileSystem(uri, true);
    }

    @Override
    public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        @SuppressWarnings("resource")
        SeekableByteChannel seekableByteChannel = newByteChannel(path, options, attrs);
        return new FileChannel() {
            @Override
            protected void implCloseChannel() throws IOException {
                seekableByteChannel.close();
            }

            @Override
            public int read(ByteBuffer dst) throws IOException {
                int bytesRead = seekableByteChannel.read(dst);
                meterRegistry.counter("astra.s3.nio.bytesRead").increment(bytesRead);
                return bytesRead;
            }

            @Override
            public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                return seekableByteChannel.write(src);
            }

            @Override
            public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public long position() throws IOException {
                return seekableByteChannel.position();
            }

            @Override
            public FileChannel position(long newPosition) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public long size() throws IOException {
                meterRegistry.counter("astra.s3.nio.sizeCallCounter").increment();
                return seekableByteChannel.size();
            }

            @Override
            public FileChannel truncate(long size) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public void force(boolean metaData) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public int read(ByteBuffer dst, long position) throws IOException {
                int bytesRead = seekableByteChannel.position(position).read(dst);
                meterRegistry.counter("astra.s3.nio.bytesReadWithPos").increment(bytesRead);
                return bytesRead;
            }

            @Override
            public int write(ByteBuffer src, long position) throws IOException {
                return seekableByteChannel.position(position).write(src);
            }

            @Override
            public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public FileLock lock(long position, long size, boolean shared) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public FileLock tryLock(long position, long size, boolean shared) throws IOException {
                throw new NotImplementedException();
            }
        };

    }

    /**
     * Returns an existing {@code FileSystem} created by this provider.
     *
     * <p> This method returns a reference to a {@code FileSystem} that was
     * created by invoking the {@link #newFileSystem(URI, Map) newFileSystem(URI,Map)}
     * method. File systems created the {@link #newFileSystem(Path, Map)
     * newFileSystem(Path,Map)} method are not returned by this method.
     * The file system is identified by its {@code URI}. Its exact form
     * is highly provider dependent. In the case of the default provider the URI's
     * path component is {@code "/"} and the authority, query and fragment components
     * are undefined (Undefined components are represented by {@code null}).
     *
     * <p> Once a file system created by this provider is {@link
     * FileSystem#close closed} it is provider-dependent if this
     * method returns a reference to the closed file system or throws {@link
     * FileSystemNotFoundException}. If the provider allows a new file system to
     * be created with the same URI as a file system it previously created then
     * this method throws the exception if invoked after the file system is
     * closed (and before a new instance is created by the {@link #newFileSystem
     * newFileSystem} method).
     *
     * <p> If a security manager is installed then a provider implementation
     * may require to check a permission before returning a reference to an
     * existing file system. In the case of the {@link FileSystems#getDefault
     * default} file system, no permission check is required.
     *
     * @param uri URI reference
     * @return The file system
     * @throws IllegalArgumentException    If the pre-conditions for the {@code uri} parameter aren't met
     * @throws FileSystemNotFoundException If the file system does not exist
     * @throws SecurityException           If a security manager is installed, and it denies an unspecified
     *                                     permission.
     */
    @Override
    public FileSystem getFileSystem(URI uri) {
        return getFileSystem(uri, false);
    }

    /**
     * Return a {@code Path} object by converting the given {@link URI}. The
     * resulting {@code Path} is associated with a {@link FileSystem} that
     * already exists or is constructed automatically.
     *
     * <p> The exact form of the URI is file system provider dependent. In the
     * case of the default provider, the URI scheme is {@code "file"} and the
     * given URI has a non-empty path component, and undefined query, and
     * fragment components. The resulting {@code Path} is associated with the
     * default {@link FileSystems#getDefault default} {@code FileSystem}.
     *
     * <p> If a security manager is installed then a provider implementation
     * may require to check a permission. In the case of the {@link
     * FileSystems#getDefault default} file system, no permission check is
     * required.
     *
     * @param uri The URI to convert. Must not be null.
     * @return The resulting {@code Path}
     * @throws IllegalArgumentException    If the URI scheme does not identify this provider or other
     *                                     preconditions on the uri parameter do not hold
     * @throws FileSystemNotFoundException The file system, identified by the URI, does not exist and
     *                                     cannot be created automatically
     * @throws SecurityException           If a security manager is installed, and it denies an unspecified
     *                                     permission.
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public Path getPath(URI uri) throws IllegalArgumentException, FileSystemNotFoundException, SecurityException {
        Objects.requireNonNull(uri);
        return getFileSystem(uri, true).getPath(uri.getScheme() + ":/" + uri.getPath());
    }

    /**
     * Opens or creates a file, returning a seekable byte channel to access the
     * file. This method works in exactly the manner specified by the {@link
     * Files#newByteChannel(Path, Set, FileAttribute[])} method.
     *
     * @param path    the path to the file to open or create
     * @param options options specifying how the file is opened
     * @param attrs   an optional list of file attributes to set atomically when
     *                creating the file
     * @return a new seekable byte channel
     * @throws IllegalArgumentException      if the set contains an invalid combination of options
     * @throws UnsupportedOperationException if an unsupported open option is specified or the array contains
     *                                       attributes that cannot be set atomically when creating the file
     * @throws FileAlreadyExistsException    if a file of that name already exists and the {@link
     *                                       StandardOpenOption#CREATE_NEW CREATE_NEW} option is specified
     *                                       <i>(optional specific exception)</i>
     * @throws IOException                   if an I/O error occurs
     * @throws SecurityException             In the case of the default provider, and a security manager is
     *                                       installed, the {@link SecurityManager#checkRead(String) checkRead}
     *                                       method is invoked to check read access to the path if the file is
     *                                       opened for reading. The {@link SecurityManager#checkWrite(String)
     *                                       checkWrite} method is invoked to check write access to the path
     *                                       if the file is opened for writing. The {@link
     *                                       SecurityManager#checkDelete(String) checkDelete} method is
     *                                       invoked to check delete access if the file is opened with the
     *                                       {@code DELETE_ON_CLOSE} option.
     */
    @Override
    public SeekableByteChannel newByteChannel(
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    ) throws IOException {
        if (Objects.isNull(options)) {
            options = Collections.emptySet();
        }

        final var s3Path = checkPath(path);
        final var fs = s3Path.getFileSystem();
        final var channel = new S3SeekableByteChannel(s3Path, fs.client(), options);

        fs.registerOpenChannel(channel);

        return channel;
    }

    /**
     * Opens a directory, returning a {@code DirectoryStream} to iterate over
     * the entries in the directory. This method works in exactly the manner
     * specified by the {@link
     * Files#newDirectoryStream(Path, DirectoryStream.Filter)}
     * method.
     *
     * @param dir    the path to the directory
     * @param filter the directory stream filter
     * @return a new and open {@code DirectoryStream} object
     */
    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        var s3Path = checkPath(dir);

        var dirName = s3Path.toAbsolutePath().getKey();
        if (!s3Path.isDirectory()) {
            dirName = dirName + PATH_SEPARATOR;
        }

        try {
            return new S3DirectoryStream(s3Path.getFileSystem(), s3Path.bucketName(), dirName, filter);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof ExecutionException) {
                var cause = (Exception) e.getCause().getCause();
                if (cause instanceof NoSuchBucketException) {
                    throw new FileSystemNotFoundException("Bucket '" + s3Path.bucketName() + "' not found: NoSuchBucket");
                }
                if (cause instanceof S3Exception && ((S3Exception) cause).statusCode() == 403) {
                    throw new AccessDeniedException("Access to bucket '" + s3Path.bucketName() + "' denied", s3Path.toString(),
                            cause.getMessage());
                }
                throw new IOException(cause.getMessage(), cause);
            }

            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Creates a new directory. This method works in exactly the manner
     * specified by the {@link Files#createDirectory} method.
     *
     * @param dir   the directory to create
     * @param attrs an optional list of file attributes to set atomically when
     *              creating the directory
     */
    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        var s3Directory = checkPath(dir);
        if (s3Directory.toString().equals("/") || s3Directory.toString().isEmpty()) {
            throw new FileAlreadyExistsException("Root directory already exists");
        }
        var directoryKey = s3Directory.toRealPath(NOFOLLOW_LINKS).getKey();
        if (!directoryKey.endsWith(PATH_SEPARATOR) && !directoryKey.isEmpty()) {
            directoryKey = directoryKey + PATH_SEPARATOR;
        }

        var timeOut = TIMEOUT_TIME_LENGTH_1;
        final var unit = MINUTES;

        try (S3AsyncClient client = s3Directory.getFileSystem().client()) {
            client.putObject(
                PutObjectRequest.builder()
                    .bucket(s3Directory.bucketName())
                    .key(directoryKey)
                    .build(),
                AsyncRequestBody.empty()
            ).get(timeOut, unit);
        } catch (TimeoutException e) {
            throw logAndGenerateExceptionOnTimeOut(logger, "createDirectory", timeOut, unit);
        } catch (ExecutionException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes a file. This method works in exactly the  manner specified by the
     * {@link Files#delete} method.
     *
     * @param path the path to the file to delete
     */
    @Override
    public void delete(Path path) throws IOException {
        final var s3Path = checkPath(path);
        final var prefix = s3Path.toRealPath(NOFOLLOW_LINKS).getKey();
        final var bucketName = s3Path.bucketName();

        final var s3Client = s3Path.getFileSystem().client();

        var timeOut = TIMEOUT_TIME_LENGTH_1;
        final var unit = MINUTES;
        try {
            var keys = getContainedObjectBatches(s3Client, bucketName, prefix, timeOut, unit);

            for (var keyList : keys) {
                s3Client.deleteObjects(DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(Delete.builder()
                            .objects(keyList)
                            .build())
                        .build())
                    .get(timeOut, unit);
            }
        } catch (TimeoutException e) {
            throw logAndGenerateExceptionOnTimeOut(logger, "delete", timeOut, unit);
        } catch (ExecutionException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Copy a file to a target file. This method works in exactly the manner
     * specified by the {@link Files#copy(Path, Path, CopyOption[])} method
     * except that both the source and target paths must be associated with
     * this provider.
     * <br>
     * Our implementation will also copy the content of the directory via batched copy operations. This is a variance
     * from some other implementations such as `UnixFileSystemProvider` where directory contents are not copied and the
     * use of the {@code walkFileTree} is suggested to perform deep copies. In S3 this could result in an explosion
     * of API calls which would be both expensive in time and possibly money.
     *
     * @param source  the path to the file to copy
     * @param target  the path to the target file
     * @param options options specifying how the copy should be done
     */
    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        // If both paths point to the same object, this is a no-op
        if (source.equals(target)) {
            return;
        }

        var s3SourcePath = checkPath(source);
        var s3TargetPath = checkPath(target);

        final var s3Client = s3SourcePath.getFileSystem().client();
        final var sourceBucket = s3SourcePath.bucketName();

        var sourcePrefix = s3SourcePath.toRealPath(NOFOLLOW_LINKS).getKey();

        final var timeOut = TIMEOUT_TIME_LENGTH_1;
        final var unit = MINUTES;

        var fileExistsAndCannotReplace = cannotReplaceAndFileExistsCheck(options, s3Client);

        try {
            var sourceKeys = getContainedObjectBatches(s3Client, sourceBucket, sourcePrefix, timeOut, unit);
            final var prefixWithSeparator = sourcePrefix + PATH_SEPARATOR;
            try (var s3TransferManager = S3TransferManager.builder().s3Client(s3Client).build()) {
                for (var keyList : sourceKeys) {
                    for (var objectIdentifier : keyList) {
                        copyKey(objectIdentifier.key(), prefixWithSeparator, sourceBucket, s3TargetPath, s3TransferManager,
                            fileExistsAndCannotReplace).get(timeOut, unit);
                    }
                }
            }
        } catch (TimeoutException e) {
            throw logAndGenerateExceptionOnTimeOut(logger, "copy", timeOut, unit);
        } catch (ExecutionException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Move or rename a file to a target file. This method works in exactly the
     * manner specified by the {@link Files#move} method except that both the
     * source and target paths must be associated with this provider.
     *
     * @param source  the path to the file to move
     * @param target  the path to the target file
     * @param options options specifying how the move should be done
     */
    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        this.copy(source, target, options);
        this.delete(source);
    }

    /**
     * Tests if two paths locate the same file. This method works in exactly the
     * manner specified by the {@link Files#isSameFile} method.
     *
     * @param path  one path to the file
     * @param path2 the other path
     * @return {@code true} if, and only if, the two paths locate the same file
     * @throws IOException       if an I/O error occurs
     * @throws SecurityException In the case of the default provider, and a security manager is
     *                           installed, the {@link SecurityManager#checkRead(String) checkRead}
     *                           method is invoked to check read access to both files.
     */
    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return path.toRealPath(NOFOLLOW_LINKS).equals(path2.toRealPath(NOFOLLOW_LINKS));
    }

    /**
     * There are no hidden files in S3
     *
     * @param path the path to the file to test
     * @return {@code false} always
     */
    @Override
    public boolean isHidden(Path path) {
        return false;
    }

    /**
     * S3 buckets don't have partitions or volumes so there are no file stores
     *
     * @param path the path to the file
     * @return {@code null} always
     */
    @Override
    public FileStore getFileStore(Path path) {
        return null;
    }

    /**
     * Checks the existence, and optionally the accessibility, of a file.
     *
     * <p> This method may be used by the {@link Files#isReadable isReadable},
     * {@link Files#isWritable isWritable} and {@link Files#isExecutable
     * isExecutable} methods to check the accessibility of a file.
     *
     * <p> This method checks the existence of a file and that this Java virtual
     * machine has appropriate privileges that would allow it to access the file
     * according to all the access modes specified in the {@code modes} parameter
     * as follows:
     * <br>
     * <table class="striped">
     * <caption style="display:none">Access Modes</caption>
     * <thead>
     * <tr> <th scope="col">Value</th> <th scope="col">Description</th> </tr>
     * </thead>
     * <tbody>
     * <tr>
     *   <th scope="row"> {@link AccessMode#READ READ} </th>
     *   <td> Checks that the file exists and that the Java virtual machine has
     *     permission to read the file. </td>
     * </tr>
     * <tr>
     *   <th scope="row"> {@link AccessMode#WRITE WRITE} </th>
     *   <td> Checks that the file exists and that the Java virtual machine has
     *     permission to write to the file, </td>
     * </tr>
     * <tr>
     *   <th scope="row"> {@link AccessMode#EXECUTE EXECUTE} </th>
     *   <td> Checks that the file exists and that the Java virtual machine has
     *     permission to {@link Runtime#exec execute} the file. The semantics
     *     may differ when checking access to a directory. For example, on UNIX
     *     systems, checking for {@code EXECUTE} access checks that the Java
     *     virtual machine has permission to search the directory in order to
     *     access file or subdirectories. </td>
     * </tr>
     * </tbody>
     * </table>
     *
     * <p> If the {@code modes} parameter is of length zero, then the existence
     * of the file is checked.
     *
     * <p> This method follows symbolic links if the file referenced by this
     * object is a symbolic link. Depending on the implementation, this method
     * may require reading file permissions, access control lists, or other
     * file attributes in order to check the effective access to the file. To
     * determine the effective access to a file may require access to several
     * attributes and so in some implementations this method may not be atomic
     * with respect to other file system operations.
     *
     * @param path  the path to the file to check
     * @param modes The access modes to check; may have zero elements
     * @throws UnsupportedOperationException an implementation is required to support checking for
     *                                       {@code READ}, {@code WRITE}, and {@code EXECUTE} access. This
     *                                       exception is specified to allow for the {@code Access} enum to
     *                                       be extended in future releases.
     * @throws NoSuchFileException           if a file does not exist <i>(optional specific exception)</i>
     * @throws AccessDeniedException         the requested access would be denied or the access cannot be
     *                                       determined because the Java virtual machine has insufficient
     *                                       privileges or other reasons. <i>(optional specific exception)</i>
     * @throws IOException                   if an I/O error occurs
     * @throws SecurityException             In the case of the default provider, and a security manager is
     *                                       installed, the {@link SecurityManager#checkRead(String) checkRead}
     *                                       is invoked when checking read access to the file or only the
     *                                       existence of the file, the {@link SecurityManager#checkWrite(String)
     *                                       checkWrite} is invoked when checking write access to the file,
     *                                       and {@link SecurityManager#checkExec(String) checkExec} is invoked
     *                                       when checking execute access.
     */
    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        // warn if AccessModes includes WRITE or EXECUTE
        for (var mode : modes) {
            if (mode == AccessMode.WRITE || mode == AccessMode.EXECUTE) {
                logger.warn("checkAccess: AccessMode '{}' is currently not checked by S3FileSystemProvider", mode);
            }
        }

        final var s3Path = checkPath(path.toRealPath(NOFOLLOW_LINKS));
        final var response = getCompletableFutureForHead(s3Path);

        var timeOut = TimeOutUtils.TIMEOUT_TIME_LENGTH_1;
        var unit = MINUTES;

        try {
            var ioException = response.handleAsync((resp, ex) -> {
                if (ex != null) {
                    return new IOException(ex);
                }

                // possible success but ListObjectsV2Responses can be empty so need to check that.
                if (resp instanceof ListObjectsV2Response) {
                    var listResp = (ListObjectsV2Response) resp;

                    if (listResp.hasCommonPrefixes() && !listResp.commonPrefixes().isEmpty()) {
                        logger.debug("checkAccess - common prefixes: access is OK");
                        return null;
                    }

                    if (listResp.hasContents() && !listResp.contents().isEmpty()) {
                        logger.debug("checkAccess - contents: access is OK");
                        return null;
                    }

                    return new NoSuchFileException(s3Path.toString());
                }

                logger.debug("checkAccess: access is OK");
                return null;
            }).get(timeOut, unit);

            // if handling the response produced an exception we throw it, access is not OK.
            if (ioException != null) {
                throw ioException;
            }
        } catch (TimeoutException e) {
            throw logAndGenerateExceptionOnTimeOut(logger, "checkAccess", timeOut, unit);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<? extends S3Response> getCompletableFutureForHead(S3Path s3Path) {
        final var fs = s3Path.getFileSystem();
        final var bucketName = fs.bucketName();
        final var s3Client = fs.client();

        final CompletableFuture<? extends S3Response> response;
        if (s3Path.equals(s3Path.getRoot())) {
            response = s3Client.headBucket(request -> request.bucket(bucketName));
        } else if (s3Path.isDirectory()) {
            response = s3Client.listObjectsV2(req -> req.bucket(bucketName).prefix(s3Path.getKey()));
        } else {
            response = s3Client.headObject(req -> req.bucket(bucketName).key(s3Path.getKey()));
        }
        return response;
    }

    /**
     * Returns a file attribute view of a given type. This method works in
     * exactly the manner specified by the {@link Files#getFileAttributeView}
     * method.
     *
     * @param <V>     type of FileAttributeView, see type
     * @param path    the path to the file
     * @param type    the {@code Class} object corresponding to the file attribute view.
     *                Must be {@code BasicFileAttributeView.class}
     * @param options ignored as there are no links in S3
     * @return a file attribute view of the specified type, or {@code null} if
     * the attribute view type is not available
     */
    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        Objects.requireNonNull(type, "the type of attribute view required cannot be null");
        var s3Path = checkPath(path);

        if (type.equals(BasicFileAttributeView.class)) {
            @SuppressWarnings("unchecked") final var v = (V) new S3BasicFileAttributeView(s3Path);
            return v;
        } else {
            throw new IllegalArgumentException("type must be BasicFileAttributeView.class");
        }
    }

    /**
     * Reads a file's attributes as a bulk operation. This method works in
     * exactly the manner specified by the {@link
     * Files#readAttributes(Path, Class, LinkOption[])} method.
     *
     * @param path    the path to the file
     * @param type    the {@code Class} of the file attributes required
     *                to read. Supported types are {@code BasicFileAttributes}
     * @param options options indicating how symbolic links are handled
     * @return the file attributes or {@code null} if {@code path} is inferred to be a directory.
     */
    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        Objects.requireNonNull(type);
        var s3Path = checkPath(path);

        if (type.equals(BasicFileAttributes.class)) {
            @SuppressWarnings("unchecked")
            var a = (A) S3BasicFileAttributes.get(s3Path, Duration.ofMinutes(TimeOutUtils.TIMEOUT_TIME_LENGTH_1));
            return a;
        } else {
            throw new UnsupportedOperationException("cannot read attributes of type: " + type);
        }
    }

    /**
     * Reads a set of file attributes as a bulk operation. Largely equivalent to
     * {@code readAttributes(Path path, Class<A> type, LinkOption... options)} where the returned object is a map of
     * method names (attributes) to values, filtered on the comma separated {@code attributes}.
     *
     * @param path       the path to the file
     * @param attributes the comma separated attributes to read. May be prefixed with "s3:"
     * @param options    ignored, S3 has no links
     * @return a map of the attributes returned; may be empty. The map's keys
     * are the attribute names, its values are the attribute values. Returns an empty map if {@code attributes} is empty,
     * or if {@code path} is inferred to be a directory.
     * @throws UnsupportedOperationException if the attribute view is not available
     * @throws IllegalArgumentException      if no attributes are specified or an unrecognized attributes is
     *                                       specified
     * @throws SecurityException             In the case of the default provider, and a security manager is
     *                                       installed, its {@link SecurityManager#checkRead(String) checkRead}
     *                                       method denies read access to the file. If this method is invoked
     *                                       to read security sensitive attributes then the security manager
     *                                       may be invoked to check for additional permissions.
     */
    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        Objects.requireNonNull(attributes);
        var s3Path = checkPath(path);

        if (s3Path.isDirectory() || attributes.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        var attributesFilter = attributesFilterFor(attributes);
        return S3BasicFileAttributes.get(s3Path, Duration.ofMinutes(TimeOutUtils.TIMEOUT_TIME_LENGTH_1)).asMap(attributesFilter);
    }

    /**
     * File attributes of S3 objects cannot be set other than by creating a new object
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
            throws UnsupportedOperationException {
        throw new UnsupportedOperationException("s3 file attributes cannot be modified by this class");
    }

    /**
     * Similar to getFileSystem(uri), but it allows to create the file system if
     * not yet created.
     *
     * @param uri    URI reference
     * @param create if true, the file system is created if not already done
     * @return The file system
     * @throws IllegalArgumentException    If the pre-conditions for the {@code uri} parameter aren't met
     * @throws FileSystemNotFoundException If the file system does not exist
     * @throws SecurityException           If a security manager is installed, and it denies an unspecified
     *                                     permission.
     */
    S3FileSystem getFileSystem(URI uri, boolean create) {
        var info = fileSystemInfo(uri);
        return FS_CACHE.computeIfAbsent(info.key(), (key) -> {
            if (!create) {
                throw new FileSystemNotFoundException("file system not found for '" + info.key() + "'");
            }

            var config = new S3NioSpiConfiguration().withEndpoint(info.endpoint()).withBucketName(info.bucket());
            if (info.accessKey() != null) {
                config.withCredentials(info.accessKey(), info.accessSecret());
            }
            return new S3FileSystem(this, config);
        });
    }

    void closeFileSystem(FileSystem fs) {
        for (var key : FS_CACHE.keySet()) {
            if (fs == FS_CACHE.get(key)) {
                try (FileSystem closeable = FS_CACHE.remove(key)) {
                    closeFileSystemIfOpen(closeable);
                    return;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        try {
            closeFileSystemIfOpen(fs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void closeFileSystemIfOpen(FileSystem fs) throws IOException {
        if (fs.isOpen()) {
            fs.close();
        }
    }


    boolean exists(S3AsyncClient s3Client, S3Path path) throws InterruptedException, TimeoutException {
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(path.bucketName()).key(path.getKey()).build())
                .get(TIMEOUT_TIME_LENGTH_1, MINUTES);
            return true;
        } catch (ExecutionException | NoSuchKeyException e) {
            logger.debug("Could not retrieve object head information", e);
            return false;
        }
    }

    /**
     * This method parses the provided URI into elements useful to address
     * and configure the access to a bucket. These are:
     * <br>
     * - key: the file system key that can be used to uniquely identify a S3
     * file systems instance (for example for caching purposes)
     * - bucket: the name of the bucked to be addressed
     * - endpoint: non default endpoint where the bucket is located
     * <br>
     * The default implementation in {@code S3FileSystemProvider} treats {@code uri}
     * strictly a AWS S3 URI (see Accessing a bucket using S3:// section <a href="in
     * ">* https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-</a>intro.html).
     * As such, it returns an empty endpoint and the name of the bucket as key.
     * <br>
     * Subclasses can override this method to implement alternative parsing of
     * the provided URI so that they can implement alternative URI schemes.
     *
     * @param uri the uri to address the bucket
     * @return the information extracted from {@code uri}
     */
    S3FileSystemInfo fileSystemInfo(URI uri) {
        return new S3FileSystemInfo(uri);
    }

    private static List<List<ObjectIdentifier>> getContainedObjectBatches(
        S3AsyncClient s3Client,
        String bucketName,
        String prefix,
        long timeOut,
        TimeUnit unit
    ) throws InterruptedException, ExecutionException, TimeoutException {
        String continuationToken = null;
        var hasMoreItems = true;
        List<List<ObjectIdentifier>> keys = new ArrayList<>();
        final var requestBuilder = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix);

        while (hasMoreItems) {
            var finalContinuationToken = continuationToken;
            var response = s3Client.listObjectsV2(
                requestBuilder.continuationToken(finalContinuationToken).build()
            ).get(timeOut, unit);
            var objects = response.contents()
                .stream()
                .filter(s3Object -> s3Object.key().equals(prefix) || s3Object.key().startsWith(prefix + PATH_SEPARATOR))
                .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                .collect(Collectors.toList());
            if (!objects.isEmpty()) {
                keys.add(objects);
            }
            hasMoreItems = response.isTruncated();
            continuationToken = response.nextContinuationToken();
        }
        return keys;
    }

    private static Predicate<String> attributesFilterFor(String attributes) {
        if (attributes.equals("*") || attributes.equals("s3")) {
            return x -> true;
        }
        final var attrSet = Arrays.stream(attributes.split(","))
            .map(attr -> attr.replaceAll("^s3:", ""))
            .collect(Collectors.toSet());
        return attrSet::contains;
    }

    private CompletableFuture<CompletedCopy> copyKey(
        String sourceObjectIdentifierKey,
        String sourcePrefix,
        String sourceBucket,
        S3Path targetPath,
        S3TransferManager transferManager,
        Function<S3Path, Boolean> fileExistsAndCannotReplaceFn
    ) throws FileAlreadyExistsException {
        final var sanitizedIdKey = sourceObjectIdentifierKey.replaceFirst(sourcePrefix, "");
        var resolvedS3TargetPath = targetPath.resolve(sanitizedIdKey);

        if (fileExistsAndCannotReplaceFn.apply(resolvedS3TargetPath)) {
            throw new FileAlreadyExistsException("File already exists at the target key");
        }

        return transferManager.copy(CopyRequest.builder()
            .copyObjectRequest(CopyObjectRequest.builder()
                .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                .sourceBucket(sourceBucket)
                .sourceKey(sourceObjectIdentifierKey)
                .destinationBucket(resolvedS3TargetPath.bucketName())
                .destinationKey(resolvedS3TargetPath.getKey())
                .build())
            .build()).completionFuture();
    }

    private Function<S3Path, Boolean> cannotReplaceAndFileExistsCheck(CopyOption[] options, S3AsyncClient s3Client) {
        final var canReplaceFile = Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING);

        return (S3Path destination) -> {
            if (canReplaceFile) {
                return false;
            }
            try {
                return exists(s3Client, destination);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        };
    }

    static S3Path checkPath(Path obj) {
        Objects.requireNonNull(obj);
        if (!(obj instanceof S3Path)) {
            throw new ProviderMismatchException();
        }
        return (S3Path) obj;
    }
}
