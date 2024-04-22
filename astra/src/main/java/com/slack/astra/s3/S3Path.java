/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;

import com.slack.astra.s3.config.S3NioSpiConfiguration;

import java.io.File;
import java.io.IOError;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static com.slack.astra.s3.Constants.PATH_SEPARATOR;
import static com.slack.astra.s3.S3FileSystemProvider.checkPath;

@SuppressWarnings("NullableProblems")
class S3Path implements Path {

    private final S3FileSystem fileSystem;
    private final PosixLikePathRepresentation pathRepresentation;

    private S3Path(S3FileSystem fileSystem, PosixLikePathRepresentation pathRepresentation) {
        this.fileSystem = fileSystem;
        this.pathRepresentation = pathRepresentation;
    }

    /**
     * Construct a path using the same filesystem (bucket) as this path
     */
    private S3Path from(String path) {
        return getPath(this.fileSystem, path);
    }

    /**
     * Construct a Path in the parent FileSystem using the POSIX style.
     * The path string is assumed to follow the POSIX form
     * with the "root" of the bucket being represented by "/". The supplied path should not
     * be a URI. It should not start with the string "s3:". For example, if this S3FileSystem
     * represents "{@code s3://my-bucket}" then "{@code s3://my-bucket/foo.txt}" should be addressed by the path
     * "/foo.txt" or by a path relative to the current working directory following POSIX conventions.
     * Further, although folders or directories don't technically exist in S3
     * the presence of a directory is implicit if "{@code s3://my-bucket/someFolder/}" contains
     * objects and the Path to this folder is therefore valid.
     *
     * <p>This library <b>DOES NOT</b> support S3 Paths that are not compliant with POSIX conventions. For example,
     * the URI {@code s3://my-bucket/../foo.txt} is legal in S3 but due to POSIX conventions it will be
     * unreachable through this API due to the special meaning of the .. directory alias in POSIX.</p>
     *
     * @param fsForBucket the filesystem for the bucket that holds this path
     * @param first       the path string or initial part of the path string, may not be null.
     *                    It may not be empty unless more is also null has zero length
     * @param more        additional strings to be joined to form the path string
     * @return a new S3Path
     * @throws InvalidPathException if the Path cannot be constructed
     */
    static S3Path getPath(S3FileSystem fsForBucket, String first, String... more) {
        if (fsForBucket == null) {
            throw new IllegalArgumentException("The S3FileSystem may not be null");
        }
        if (first == null) {
            throw new IllegalArgumentException("first element of the path may not be null");
        }

        var configuration = fsForBucket.configuration();

        first = first.trim();

        if (first.isEmpty() && !(more == null || more.length == 0)) {
            throw new IllegalArgumentException("The first element of the path may not be empty when more exists");
        }

        var scheme = fsForBucket.provider().getScheme();
        if (first.startsWith(scheme + ":/")) {
            first = removeScheme(first, scheme);
            first = removeCredentials(first, configuration);
            first = removeEndpoint(first, configuration.getEndpoint());
            first = removeBucket(first, configuration.getBucketName());
        }

        return new S3Path(fsForBucket, PosixLikePathRepresentation.of(first, more));
    }

    /**
     * Returns the file system that created this object.
     *
     * @return the file system that created this object
     */
    @Override
    public S3FileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * Tells whether this path is absolute.
     *
     * <p> An absolute path is complete in that it doesn't need to be combined
     * with other path information in order to locate a file.
     *
     * @return {@code true} if, and only if, this path is absolute
     */
    @Override
    public boolean isAbsolute() {
        return pathRepresentation.isAbsolute();
    }

    /**
     * If the path is absolute then returns the root of the path (e.g. "/") otherwise {@code null}
     *
     * @return a path representing the root component of this path,
     * or {@code null}
     */
    @Override
    public S3Path getRoot() {
        return isAbsolute() ? new S3Path(fileSystem, PosixLikePathRepresentation.ROOT) : null;
    }

    /**
     * Returns the name of the file or directory denoted by this path as a
     * {@code Path} object. The file name is the <em>farthest</em> element from
     * the root in the directory hierarchy.
     *
     * @return a path representing the name of the file or directory, or
     * {@code null} if this path has zero elements
     */
    @Override
    public S3Path getFileName() {
        final var elements = pathRepresentation.elements();
        var size = elements.size();
        if (size == 0) {
            return null;
        }

        if (pathRepresentation.hasTrailingSeparator()) {
            return from(elements.get(size - 1) + PATH_SEPARATOR);
        } else {
            return from(elements.get(size - 1));
        }
    }

    /**
     * Returns the <em>parent path</em>, or {@code null} if this path does not
     * have a parent.
     *
     * <p> The parent of this path object consists of this path's root
     * component, if any, and each element in the path except for the
     * <em>farthest</em> from the root in the directory hierarchy. This method
     * does not access the file system; the path or its parent may not exist.
     * Furthermore, this method does not eliminate special names such as "{@code .}"
     * and "{@code ..}" that may be used in some implementations. On UNIX for example,
     * the parent of "{@code /a/b/c}" is "{@code /a/b}", and the parent of
     * {@code "x/y/.}" is "{@code x/y}". This method may be used with the {@link
     * #normalize normalize} method, to eliminate redundant names, for cases where
     * <em>shell-like</em> navigation is required.
     *
     * <p> If this path has one or more elements, and no root component, then
     * this method is equivalent to evaluating the expression:
     * <blockquote><pre>
     * subpath(0,&nbsp;getNameCount()-1);
     * </pre></blockquote>
     *
     * @return a path representing the path's parent
     */
    @Override
    public S3Path getParent() {
        var size = pathRepresentation.elements().size();
        if (this.equals(getRoot()) || size < 1) {
            return null;
        }
        if (pathRepresentation.isAbsolute() && size == 1) {
            return getRoot();
        }
        return subpath(0, getNameCount() - 1);
    }

    /**
     * Returns the number of name elements in the path.
     *
     * @return the number of elements in the path, or {@code 0} if this path
     * only represents a root component
     */
    @Override
    public int getNameCount() {
        return pathRepresentation.elements().size();
    }

    /**
     * Returns a name element of this path as a {@code Path} object.
     *
     * <p> The {@code index} parameter is the index of the name element to return.
     * The element that is <em>closest</em> to the root in the directory hierarchy
     * has the index {@code 0}. The element that is <em>farthest</em> from the root
     * has the index {@link #getNameCount count}{@code -1}.
     *
     * @param index the index of the element
     * @return the name element
     * @throws IllegalArgumentException if {@code index} is negative, {@code index} is greater than or
     *                                  equal to the number of elements, or this path has zero name
     *                                  elements
     */
    @Override
    public S3Path getName(int index) {
        final var elements = pathRepresentation.elements();
        if (index < 0 || index >= elements.size()) {
            throw new IllegalArgumentException("index must be >= 0 and <= the number of path elements");
        }
        return subpath(index, index + 1);
    }

    /**
     * Returns a relative {@code Path} that is a subsequence of the name
     * elements of this path.
     *
     * <p> The {@code beginIndex} and {@code endIndex} parameters specify the
     * subsequence of name elements. The name that is <em>closest</em> to the root
     * in the directory hierarchy has the index {@code 0}. The name that is
     * <em>farthest</em> from the root has the index {@link #getNameCount
     * count}{@code -1}. The returned {@code Path} object has the name elements
     * that begin at {@code beginIndex} and extend to the element at index {@code
     * endIndex-1}.
     *
     * @param beginIndex the index of the first element, inclusive
     * @param endIndex   the index of the last element, exclusive
     * @return a new {@code Path} object that is a subsequence of the name
     * elements in this {@code Path}
     * @throws IllegalArgumentException if {@code beginIndex} is negative, or greater than or equal to
     *                                  the number of elements. If {@code endIndex} is less than or
     *                                  equal to {@code beginIndex}, or larger than the number of elements.
     */
    @Override
    public S3Path subpath(int beginIndex, int endIndex) {
        final var size = pathRepresentation.elements().size();
        if (beginIndex < 0) {
            throw new IllegalArgumentException("begin index may not be < 0");
        }
        if (beginIndex >= size) {
            throw new IllegalArgumentException("begin index may not be >= the number of path elements");
        }
        if (endIndex > size) {
            throw new IllegalArgumentException("end index may not be > the number of path elements");
        }
        if (endIndex <= beginIndex) {
            throw new IllegalArgumentException("end index may not be <= the begin index");
        }

        var path = String.join(PATH_SEPARATOR, pathRepresentation.elements().subList(beginIndex, endIndex));
        if (endIndex == size && !pathRepresentation.hasTrailingSeparator()) {
            return from(path);
        } else {
            return from(path + PATH_SEPARATOR);
        }
    }

    /**
     * Tests if this path starts with the given path.
     *
     * <p> This path <em>starts</em> with the given path if this path's root
     * component <em>starts</em> with the root component of the given path,
     * and this path starts with the same name elements as the given path.
     * If the given path has more name elements than this path then {@code false}
     * is returned.
     *
     * <p> If this path does
     * not have a root component and the given path has a root component then
     * this path does not start with the given path.
     *
     * <p> If the given path is associated with a different {@code FileSystem} (s3 bucket)
     * to this path then {@code false} is returned.
     *
     * @param other the given path
     * @return {@code true} if this path starts with the given path; otherwise
     * {@code false}
     */
    @Override
    public boolean startsWith(Path other) {
        return this.equals(other) ||
            this.fileSystem.equals(other.getFileSystem()) &&
                this.isAbsolute() == other.isAbsolute() &&
                this.getNameCount() >= other.getNameCount() &&
                this.subpath(0, other.getNameCount()).equals(other);
    }

    /**
     * Tests if this path starts with a {@code Path}, constructed by converting
     * the given path string, in exactly the manner specified by the {@link
     * #startsWith(Path) startsWith(Path)} method.
     *
     * @param other the given path string
     * @return {@code true} if this path starts with the given path; otherwise
     * {@code false}
     * @throws InvalidPathException If the path string cannot be converted to a Path.
     */
    @Override
    public boolean startsWith(String other) {
        return startsWith(from(other));
    }

    /**
     * Tests if this path ends with the given path.
     *
     * <p> If the given path has <em>N</em> elements, and no root component,
     * and this path has <em>N</em> or more elements, then this path ends with
     * the given path if the last <em>N</em> elements of each path, starting at
     * the element farthest from the root, are equal.
     *
     * <p> If the given path has a root component then this path ends with the
     * given path if the root component of this path <em>ends with</em> the root
     * component of the given path, and the corresponding elements of both paths
     * are equal. If the two paths are equal then they can be said to end with each other. If this path
     * does not have a root component and the given path has a root component
     * then this path does not end with the given path.
     *
     * <p> If the given path is associated with a different {@code FileSystem}
     * to this path then {@code false} is returned.
     *
     * @param other the given path
     * @return {@code true} if this path ends with the given path; otherwise
     * {@code false}
     */
    @Override
    public boolean endsWith(Path other) {
        return this.equals(other) ||
            this.fileSystem == other.getFileSystem() &&
                this.getNameCount() >= other.getNameCount() &&
                this.subpath(this.getNameCount() - other.getNameCount(), this.getNameCount()).equals(other);
    }

    /**
     * Tests if this path ends with a {@code Path}, constructed by converting
     * the given path string, in exactly the manner specified by the {@link
     * #endsWith(Path) endsWith(Path)} method. On UNIX for example, the path
     * "{@code foo/bar}" ends with "{@code foo/bar}" and "{@code bar}". It does
     * not end with "{@code r}" or "{@code /bar}". Note that trailing separators
     * are not taken into account, and so invoking this method on the {@code
     * Path}"{@code foo/bar}" with the {@code String} "{@code bar/}" returns
     * {@code true}.
     *
     * @param other the given path string
     * @return {@code true} if this path ends with the given path; otherwise
     * {@code false}
     * @throws InvalidPathException If the path string cannot be converted to a Path.
     */
    @Override
    public boolean endsWith(String other) {
        return endsWith(from(other));
    }

    /**
     * Returns a path that is this path with redundant name elements eliminated.
     * All occurrences of "{@code .}" are considered redundant. If a "{@code ..}" is preceded by a
     * non-"{@code ..}" name then both names are considered redundant (the
     * process to identify such names is repeated until it is no longer
     * applicable).
     *
     * <p> This method does not access the file system; the path may not locate
     * a file that exists. Eliminating "{@code ..}" and a preceding name from a
     * path may result in the path that locates a different file than the original
     * path. This can arise when the preceding name is a symbolic link.
     *
     * @return the resulting path or this path if it does not contain
     * redundant name elements; an empty path is returned if this path
     * does have a root component and all name elements are redundant
     * @see #getParent
     * @see #toRealPath
     */
    @Override
    public S3Path normalize() {
        if (pathRepresentation.isRoot()) {
            return this;
        }

        var directory = pathRepresentation.isDirectory();

        final var elements = pathRepresentation.elements();
        final var realElements = new LinkedList<String>();

        if (this.isAbsolute()) {
            realElements.add(PATH_SEPARATOR);
        }

        for (var element : elements) {
            if (element.equals(".")) {
                continue;
            }
            if (element.equals("..")) {
                if (!realElements.isEmpty()) {
                    realElements.removeLast();
                }
                continue;
            }

            if (directory) {
                realElements.addLast(element + "/");
            } else {
                realElements.addLast(element);
            }
        }
        return S3Path.getPath(fileSystem, String.join(PATH_SEPARATOR, realElements));
    }

    /**
     * Resolve the given path against this path.
     *
     * <p> If the {@code other} parameter is an {@link #isAbsolute() absolute}
     * path then this method trivially returns {@code other}. If {@code other}
     * is an <i>empty path</i> then this method trivially returns this path.
     * Otherwise, this method considers this path to be a directory and resolves
     * the given path against this path by
     * <em>joining</em> the given path to this path with the addition of a separator ('/') and returns a resulting path
     * that {@link #endsWith ends} with the given (other) path.
     *
     * @param other the path to resolve against this path
     * @return the resulting path
     * @throws ProviderMismatchException if {@code other} is {@code null} or if it is not an instance of {@code S3Path}
     * @throws IllegalArgumentException  if {@code other} is NOT and instance of an {@code S3Path}
     * @see #relativize
     */
    @Override
    public S3Path resolve(Path other) {
        var s3Other = checkPath(other);

        if (!this.bucketName().equals(s3Other.bucketName())) {
            throw new IllegalArgumentException("S3Paths cannot be resolved when they are from different buckets");
        }

        if (s3Other.isAbsolute()) {
            return s3Other;
        }
        if (s3Other.isEmpty()) {
            return this;
        }

        String concatenatedPath;
        if (!this.pathRepresentation.hasTrailingSeparator()) {
            concatenatedPath = this + PATH_SEPARATOR + s3Other;
        } else {
            concatenatedPath = this.toString() + s3Other;
        }

        return from(concatenatedPath);
    }

    /**
     * Converts a given path string to a {@code S3Path} and resolves it against
     * this {@code S3Path} in exactly the manner specified by the {@link
     * #resolve(Path) resolve} method.
     *
     * @param other the path string to resolve against this path
     * @return the resulting path
     * @throws InvalidPathException if the path string cannot be converted to a Path.
     * @see FileSystem#getPath
     */
    @Override
    public S3Path resolve(String other) {
        return resolve(from(other));
    }

    /**
     * Resolves the given path against this path's {@link #getParent parent}
     * path. This is useful where a file name needs to be <i>replaced</i> with
     * another file name. For example, suppose that the name separator is
     * "{@code /}" and a path represents "{@code dir1/dir2/foo}", then invoking
     * this method with the {@code Path} "{@code bar}" will result in the {@code
     * Path} "{@code dir1/dir2/bar}". If this path does not have a parent path,
     * or {@code other} is {@link #isAbsolute() absolute}, then this method
     * returns {@code other}. If {@code other} is an empty path then this method
     * returns this path's parent, or where this path doesn't have a parent, the
     * empty path.
     *
     * @param other the path to resolve against this path's parent
     * @return the resulting path
     * @see #resolve(Path)
     */
    @Override
    public S3Path resolveSibling(Path other) {
        return getParent().resolve(other);
    }

    /**
     * Converts a given path string to a {@code Path} and resolves it against
     * this path's {@link #getParent parent} path in exactly the manner
     * specified by the {@link #resolveSibling(Path) resolveSibling} method.
     *
     * @param other the path string to resolve against this path's parent
     * @return the resulting path
     * @throws InvalidPathException if the path string cannot be converted to a Path.
     * @see FileSystem#getPath
     */
    @Override
    public S3Path resolveSibling(String other) {
        return getParent().resolve(other);
    }

    /**
     * Constructs a relative path between this path and a given path.
     *
     * <p> Relativization is the inverse of {@link #resolve(Path) resolution}.
     * This method attempts to construct a {@link #isAbsolute relative} path
     * that when {@link #resolve(Path) resolved} against this path, yields a
     * path that locates the same file as the given path. For example, on UNIX,
     * if this path is {@code "/a/b"} and the given path is {@code "/a/b/c/d"}
     * then the resulting relative path would be {@code "c/d"}. Where this
     * path and the given path do not have a {@link #getRoot root} component,
     * then a relative path can be constructed. A relative path cannot be
     * constructed if only one of the paths have a root component. Where both
     * paths have a root component then it is implementation dependent if a
     * relative path can be constructed. If this path and the given path are
     * {@link #equals equal} then an <i>empty path</i> is returned.
     *
     * @param other the path to relativize against this path
     * @return the resulting relative path, or an empty path if both paths are
     * equal
     * @throws IllegalArgumentException if {@code other} is not a {@code Path} that can be relativized
     *                                  against this path
     */
    @Override
    public S3Path relativize(Path other) {
        var otherPath = checkPath(other);

        if (this.equals(otherPath)) {
            return from("");
        }

        if (this.isAbsolute() != otherPath.isAbsolute()) {
            throw new IllegalArgumentException("to obtain a relative path both must be absolute or both must be relative");
        }
        if (!Objects.equals(this.bucketName(), otherPath.bucketName())) {
            throw new IllegalArgumentException("cannot relativize S3Paths from different buckets");
        }

        if (this.isEmpty()) {
            return otherPath;
        }

        var nameCount = this.getNameCount();
        var otherNameCount = otherPath.getNameCount();

        var limit = Math.min(nameCount, otherNameCount);
        var differenceCount = getDifferenceCount(otherPath, limit);

        var parentDirCount = nameCount - differenceCount;
        if (differenceCount < otherNameCount) {
            return getRelativePathFromDifference(otherPath, otherNameCount, differenceCount, parentDirCount);
        }

        var relativePath = new char[parentDirCount * 3 - 1];
        var index = 0;
        while (parentDirCount > 0) {
            relativePath[index++] = '.';
            relativePath[index++] = '.';
            if (parentDirCount > 1) {
                relativePath[index++] = '/';
            }
            parentDirCount--;
        }

        return new S3Path(getFileSystem(), new PosixLikePathRepresentation(relativePath));
    }

    private S3Path getRelativePathFromDifference(S3Path otherPath, int otherNameCount, int differenceCount, int parentDirCount) {
        Objects.requireNonNull(otherPath);
        var remainingSubPath = otherPath.subpath(differenceCount, otherNameCount);

        if (parentDirCount == 0) {
            return remainingSubPath;
        }

        // we need to pop up some directories (each of which needs three characters ../) then append the remaining sub-path
        var relativePathSize = parentDirCount * 3 + remainingSubPath.pathRepresentation.toString().length();

        if (otherPath.isEmpty()) {
            relativePathSize--;
        }

        var relativePath = new char[relativePathSize];
        var index = 0;
        while (parentDirCount > 0) {
            relativePath[index++] = '.';
            relativePath[index++] = '.';
            if (otherPath.isEmpty()) {
                if (parentDirCount > 1) {
                    relativePath[index++] = '/';
                }
            } else {
                relativePath[index++] = '/';
            }
            parentDirCount--;
        }
        System.arraycopy(remainingSubPath.pathRepresentation.chars(), 0, relativePath, index,
            remainingSubPath.pathRepresentation.chars().length);

        return new S3Path(getFileSystem(), new PosixLikePathRepresentation(relativePath));
    }

    private int getDifferenceCount(Path other, int limit) {
        var i = 0;
        while (i < limit) {
            if (!this.getName(i).equals(other.getName(i))) {
                break;
            }
            i++;
        }
        return i;
    }

    private boolean isEmpty() {
        return pathRepresentation.toString().isEmpty();
    }

    /**
     * Returns a URI to represent this path.
     *
     * <p> This method constructs an absolute and normalized {@link URI} with a {@link
     * URI#getScheme() scheme} equal to the URI scheme that identifies the
     * provider (s3). Please note that the returned URI is a well formed URI,
     * which means all special characters (e.g. blanks, %, ?, etc.) are encoded.
     * This may result in a different string from the real path (object key),
     * which instead allows those characters.
     * <p>
     * For instance, the S3 URI "s3://mybucket/with space and %" is a valid S3
     * object key, which must be encoded when creating a Path and that will be
     * encoded when creating a URI. E.g.:
     *
     * <pre>
     * {@code
     * S3Path p = (S3Path)Paths.get("s3://mybucket/with+blank+and+%25"); // -> s3://mybucket/with blank and %
     * String s = p.toString; // -> /mybucket/with blank and %
     * URI u = p.toUri(); --> // -> s3://mybucket/with+blank+and+%25
     * ...
     * String s = p.getFileSystem().get("with space").toString(); // -> /with space
     * }
     * </pre>
     *
     * @return the URI representing this path
     * @throws IOError           if an I/O error occurs obtaining the absolute path, or where a
     *                           file system is constructed to access the contents of a file as
     *                           a file system, and the URI of the enclosing file system cannot be
     *                           obtained
     * @throws SecurityException In the case of the default provider, and a security manager
     *                           is installed, the {@link #toAbsolutePath toAbsolutePath} method
     *                           throws a security exception.
     */

    @Override
    public URI toUri() {
        Path path = toAbsolutePath().toRealPath(NOFOLLOW_LINKS);
        var elements = path.iterator();

        var uri = new StringBuilder(fileSystem.provider().getScheme() + "://");
        var endpoint = fileSystem.configuration().getEndpoint();
        if (!endpoint.isEmpty()) {
            uri.append(fileSystem.configuration().getEndpoint()).append(PATH_SEPARATOR);
        }
        uri.append(bucketName());
        elements.forEachRemaining(
            (e) -> {
                var name = e.getFileName().toString();
                if (name.endsWith(PATH_SEPARATOR)) {
                    name = name.substring(0, name.length() - 1);
                }
                uri.append(PATH_SEPARATOR).append(URLEncoder.encode(name, StandardCharsets.UTF_8));
            }
        );
        if (isDirectory()) {
            uri.append(PATH_SEPARATOR);
        }

        return URI.create(uri.toString());
    }

    /**
     * Returns a {@code Path} object representing the absolute path of this
     * path.
     *
     * <p> If this path is already {@link Path#isAbsolute absolute} then this
     * method simply returns this path. Otherwise, this method resolves the path
     * by resolving the path against the root (the top level of the bucket). The resulting path may contain redundancies
     * and may point to a non-existent location.
     *
     * @return a {@code Path} object representing the absolute path
     */
    @Override
    public S3Path toAbsolutePath() {
        if (isAbsolute()) {
            return this;
        }

        return new S3Path(fileSystem, PosixLikePathRepresentation.of(PATH_SEPARATOR, pathRepresentation.toString()));
    }

    /**
     * Returns the <em>real</em> path of an existing file.
     *
     * <p> If this path is relative then its absolute path is first obtained,
     * as if by invoking the {@link #toAbsolutePath toAbsolutePath} method.
     * When deriving the <em>real path</em>, and a
     * "{@code ..}" (or equivalent) is preceded by a non-"{@code ..}" name then
     * an implementation will cause both names to be removed.
     *
     * @param options options indicating how symbolic links are handled. S3 has no links so this will be ignored.
     * @return an absolute path represent the <em>real</em> path of the file
     * located by this object
     */
    @Override
    public S3Path toRealPath(LinkOption... options) {
        var p = this;
        if (!isAbsolute()) {
            p = toAbsolutePath();
        }

        return S3Path.getPath(fileSystem, PATH_SEPARATOR, p.normalize().toString());
    }

    /**
     * S3 Objects cannot be represented in the local file system
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public File toFile() {
        throw new UnsupportedOperationException("S3 Objects cannot be represented in the local (default) file system");
    }

    /**
     * Currently not implemented
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public WatchKey register(
        WatchService watcher,
        WatchEvent.Kind<?>[] events,
        WatchEvent.Modifier... modifiers
    ) throws UnsupportedOperationException {
        throw new UnsupportedOperationException(
            "This method is not yet supported. Please raise a feature request describing your use case"
        );
    }

    /**
     * Currently not implemented
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws UnsupportedOperationException {
        throw new UnsupportedOperationException(
            "This method is not yet supported. Please raise a feature request describing your use case"
        );
    }

    /**
     * Returns an iterator over the name elements of this path.
     *
     * <p> The first element returned by the iterator represents the name
     * element that is closest to the root in the directory hierarchy, the
     * second element is the next closest, and so on. The last element returned
     * is the name of the file or directory denoted by this path. The {@link
     * #getRoot root} component, if present, is not returned by the iterator.
     *
     * @return an iterator over the name elements of this path.
     */
    @Override
    public Iterator<Path> iterator() {
        return new S3PathIterator(pathRepresentation.elements().iterator(), pathRepresentation.isAbsolute(),
            pathRepresentation.hasTrailingSeparator());
    }

    /**
     * Compares two abstract paths lexicographically. The ordering defined by
     * this method is provider specific, and in the case of the default
     * provider, platform specific. This method does not access the file system
     * and neither file is required to exist.
     *
     * <p> This method may not be used to compare paths that are associated
     * with different file system providers.
     *
     * @param other the path compared to this path.
     * @return zero if the argument is {@link #equals equal} to this path, a
     * value less than zero if this path is lexicographically less than
     * the argument, or a value greater than zero if this path is
     * lexicographically greater than the argument
     * @throws ClassCastException if the paths are associated with different providers
     */
    @Override
    public int compareTo(Path other) {
        var o = checkPath(other);
        if (o.fileSystem != this.fileSystem) {
            throw new ClassCastException("compared S3 paths must be from the same bucket");
        }
        return this.toRealPath(NOFOLLOW_LINKS).toString().compareTo(
            o.toRealPath(NOFOLLOW_LINKS).toString());
    }

    /**
     * Tests this path for equality with the given object.
     * <p>
     * {@code true} if {@code other} is also an {@code S3Path} from the same bucket and the two paths have the same
     * real path.
     *
     * @param other the object to which this object is to be compared
     * @return {@code true} if, and only if, the given object is a {@code Path}
     * that is identical to this {@code Path}
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        return other instanceof S3Path
            && Objects.equals(((S3Path) other).bucketName(), this.bucketName())
            && Objects.equals(((S3Path) other).toRealPath(NOFOLLOW_LINKS).pathRepresentation,
            this.toRealPath(NOFOLLOW_LINKS).pathRepresentation);
    }

    /**
     * Computes a hash code for this path.
     *
     * <p> The hash code is based upon the components of the path, and
     * satisfies the general contract of the {@link Object#hashCode
     * Object.hashCode} method.
     *
     * @return the hash-code value for this path
     */
    @Override
    public int hashCode() {
        return this.bucketName().hashCode() + toRealPath(NOFOLLOW_LINKS).pathRepresentation.hashCode();
    }

    /**
     * Returns the string representation of this path.
     *
     * @return the string representation of this path
     */
    @Override
    public String toString() {
        return pathRepresentation.toString();
    }

    /**
     * The name of the S3 bucket that represents the root ("/") of this Path
     *
     * @return the bucketName, equivalent to <code>getFileSystem().bucketName()</code>
     */
    String bucketName() {
        return fileSystem.bucketName();
    }

    /**
     * Is the path inferred to be an S3 directory?
     *
     * @return true if the path can be inferred to be a directory
     */
    boolean isDirectory() {
        return pathRepresentation.isDirectory();
    }

    /**
     * The key of the object for S3. Essentially the "real path" with the "/" prefix and bucket name removed.
     *
     * @return the key
     */
    String getKey() {
        if (isEmpty()) {
            return "";
        }
        var s = toRealPath(NOFOLLOW_LINKS).toString();
        if (s.startsWith(PATH_SEPARATOR + bucketName())) {
            s = s.replaceFirst(PATH_SEPARATOR + bucketName(), "");
        }
        while (s.startsWith(PATH_SEPARATOR)) {
            s = s.substring(1);
        }
        return s;
    }

    private final class S3PathIterator implements Iterator<Path> {
        final boolean isAbsolute;
        final boolean hasTrailingSeparator;
        boolean first;
        private final Iterator<String> delegate;

        private S3PathIterator(Iterator<String> delegate, boolean isAbsolute, boolean hasTrailingSeparator) {
            this.delegate = delegate;
            this.isAbsolute = isAbsolute;
            this.hasTrailingSeparator = hasTrailingSeparator;
            first = true;
        }

        @Override
        public Path next() {
            var pathString = delegate.next();
            if (isAbsolute() && first) {
                first = false;
                pathString = PATH_SEPARATOR + pathString;
                if (!hasNext() && hasTrailingSeparator) {
                    pathString = pathString + PATH_SEPARATOR;
                }
            }

            if (hasNext() || hasTrailingSeparator) {
                pathString = pathString + PATH_SEPARATOR;
            }
            return from(pathString);
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }
    }

    private static String removeScheme(String path, String scheme) {
        return path.substring(scheme.length() + 2);
    }

    private static String removeCredentials(String first, S3NioSpiConfiguration configuration) {
        if (configuration.getCredentials() != null) {
            var credentials = configuration.getCredentials();
            var credentialsAsString = credentials.accessKeyId() + ':' + credentials.secretAccessKey();
            if (first.startsWith('/' + credentialsAsString)) {
                first = PATH_SEPARATOR + first.substring(credentialsAsString.length() + 2);
            }
        }
        return first;
    }

    private static String removeEndpoint(String first, String endpoint) {
        if (!endpoint.isEmpty() && first.startsWith(PATH_SEPARATOR + endpoint)) {
            first = first.substring(endpoint.length() + 1);
        }
        return first;
    }

    private static String removeBucket(String first, String part) {
        if (first.startsWith(PATH_SEPARATOR + part)) {
            first = first.substring(part.length() + 1);
        }
        return first;
    }
}
