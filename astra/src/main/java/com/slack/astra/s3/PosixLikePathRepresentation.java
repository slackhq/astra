/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.slack.astra.s3;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.slack.astra.s3.Constants.PATH_SEPARATOR;

/**
 * A class to hold a string representation of a Posix like path pointing to an S3 object or "directory". Provide methods
 * to obtain views of the representation according to Posix conventions and the directories implicit in S3 paths.
 * The most substantial difference with true Posix paths is that in {@code java.nio.Path, /foo/baa/} will be represented
 * as {@code /foo/baa} whereas in this representation the trailing slash must be retained to infer that {@code baa} is
 * a "directory".
 */
class PosixLikePathRepresentation {

    static final PosixLikePathRepresentation ROOT = new PosixLikePathRepresentation(PATH_SEPARATOR);
    static final PosixLikePathRepresentation EMPTY_PATH = new PosixLikePathRepresentation("");
    private static final char PATH_SEPARATOR_CHAR = PATH_SEPARATOR.charAt(0);

    private String path;

    PosixLikePathRepresentation(String path) {
        if (path == null) {
            throw new IllegalArgumentException("path may not be null");
        }
        this.path = path;
    }

    PosixLikePathRepresentation(char[] path) {
        new PosixLikePathRepresentation(new String(path));
    }

    /**
     * Construct a path representation from a series of elements. If {@code more} is {@code null} or empty then {@code first}
     * is assumed to contain the entire path. Elements will be concatenated with the standard separator to form the final
     * representation. Any redundant separators at the beginning or end of the string will be removed, e.g. {@code "/", "/foo}
     * will become {@code "/foo"}. Directory aliases {@code ".", ".."} will be retained in the path.
     * <p>The current implementation is permissive and assumes most characters are legal in the path however care should
     * be taken when using characters which although legal in S3 can cause encoding problems or may not be legal in
     * Posix paths.</p>.
     * <p>When {@code first} begins with "/" it is assumed to be absolute and the root if first is only "/" and {@code more}
     * is empty or null</p>.
     * <p>When {@code first} is the only element and it ends with "/" then a directory is assumed. Likewise if {@code more}
     * is present and the last item of {@code more} ends with "/" then a directory is assumed.</p>
     *
     * @param first the first element of the path, or the whole path if {@code more} is not defined.
     *              May not be null or empty unless {@code more} is also undefined.
     * @param more  zero or more path elements, {@code EMPTY_PATH} if {@code first} is null or empty
     * @return the representation of the path constructed from the arguments with non-redundant separators.
     */
    static PosixLikePathRepresentation of(String first, String... more) {
        if ((first == null || first.trim().isEmpty()) && !(more == null || more.length == 0)) {
            throw new IllegalArgumentException("The first element of the path may not be null or empty when more exists");
        }

        if (first == null || first.trim().isEmpty()) {
            return EMPTY_PATH;
        }

        var allParts = new LinkedList<String>();
        allParts.add(first);

        allParts.addAll(collectMore(more));

        if (allParts.peekLast() == null) {
            throw new RuntimeException("the last element of the path representation is unexpectedly null");
        }
        var endsWithSeparator = hasTrailingSeparatorString(allParts.peekLast());
        var startsWithSeparator = isAbsoluteString(allParts.peekFirst());

        var path = partsToPathString(allParts, endsWithSeparator, startsWithSeparator);

        return new PosixLikePathRepresentation(path);

    }

    private static String partsToPathString(LinkedList<String> allParts, boolean endsWithSeparator, boolean startsWithSeparator) {
        var path = allParts.stream()
            .flatMap(part -> Arrays.stream(part.split("/+")))
            .filter(p -> !p.isEmpty())
            .collect(Collectors.joining(PATH_SEPARATOR));

        if (endsWithSeparator && !hasTrailingSeparatorString(path)) {
            path = path + PATH_SEPARATOR;
        }
        if (startsWithSeparator && !isAbsoluteString(path)) {
            path = PATH_SEPARATOR + path;
        }
        return path;
    }

    private static List<String> collectMore(String[] more) {
        if (more != null && more.length != 0) {
            return Arrays.stream(more)
                .filter(Objects::nonNull)
                .filter(p -> !p.isEmpty())
                .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    /**
     * Does this path represent the root of the bucket
     *
     * @return true if and only if this path has only length 1 and contains only "/"
     */
    boolean isRoot() {
        return isRootString(path);
    }

    private static boolean isRootString(String path) {
        return path.equals(PATH_SEPARATOR);
    }

    /**
     * Is the path resolvable without any further information (e.g. not relative to some other location).
     *
     * @return true if this path begins with '/'
     */
    boolean isAbsolute() {
        return isAbsoluteString(path);
    }

    private static boolean isAbsoluteString(String path) {
        return !(path == null) &&
            !path.isEmpty() &&
            path.charAt(0) == PATH_SEPARATOR_CHAR;
    }

    /**
     * While S3 doesn't have directories, the following POSIX path representations would be considered directories when
     * S3 is used:
     * <p>
     * "/", "foo/", "./", "../", ".", "..", "", "foo/.", "foo/.."
     * <p>
     * Importantly, in a true Posix filesystem, if /foo/ is a directory then /foo is as well, however /foo in S3 cannot
     * be inferred to be a directory.
     */
    boolean isDirectory() {
        return isDirectoryString(path);
    }

    private static boolean isDirectoryString(String path) {
        return path.isEmpty()
            || hasTrailingSeparatorString(path)
            || path.equals(".")
            || path.equals("..")
            || path.endsWith(PATH_SEPARATOR_CHAR + ".")
            || path.endsWith(PATH_SEPARATOR_CHAR + "..");
    }

    boolean hasTrailingSeparator() {
        return hasTrailingSeparatorString(path);
    }

    private static boolean hasTrailingSeparatorString(String path) {
        if (path.isEmpty()) {
            return false;
        }
        return path.charAt(path.length() - 1) == PATH_SEPARATOR_CHAR;
    }

    char[] chars() {
        return path.toCharArray();
    }

    /**
     * Returns a string representation of the path representation
     *
     * @return a string.
     */
    @Override
    public String toString() {
        return path;
    }


    List<String> elements() {
        if (this.isRoot()) {
            return Collections.emptyList();
        }

        return Arrays.stream(path.split(PATH_SEPARATOR))
            .filter(s -> !s.trim().isEmpty())
            .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (PosixLikePathRepresentation) o;
        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(path);
    }
}

