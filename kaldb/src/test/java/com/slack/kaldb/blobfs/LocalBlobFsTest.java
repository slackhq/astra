package com.slack.kaldb.blobfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocalBlobFsTest {
  private File testFile;
  private File absoluteTmpDirPath;
  private File newTmpDir;
  private File nonExistentTmpFolder;

  @Before
  public void setUp() {
    absoluteTmpDirPath =
        new File(
            System.getProperty("java.io.tmpdir"), LocalBlobFsTest.class.getSimpleName() + "first");
    FileUtils.deleteQuietly(absoluteTmpDirPath);
    Assert.assertTrue(
        "Could not make directory " + absoluteTmpDirPath.getPath(), absoluteTmpDirPath.mkdir());
    try {
      testFile = new File(absoluteTmpDirPath, "testFile");
      Assert.assertTrue("Could not create file " + testFile.getPath(), testFile.createNewFile());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    newTmpDir =
        new File(
            System.getProperty("java.io.tmpdir"), LocalBlobFsTest.class.getSimpleName() + "second");
    FileUtils.deleteQuietly(newTmpDir);
    Assert.assertTrue("Could not make directory " + newTmpDir.getPath(), newTmpDir.mkdir());

    nonExistentTmpFolder =
        new File(
            System.getProperty("java.io.tmpdir"),
            LocalBlobFsTest.class.getSimpleName() + "nonExistentParent/nonExistent");

    absoluteTmpDirPath.deleteOnExit();
    newTmpDir.deleteOnExit();
    nonExistentTmpFolder.deleteOnExit();
  }

  @After
  public void tearDown() {
    absoluteTmpDirPath.delete();
    newTmpDir.delete();
  }

  @Test
  public void shouldListDirectories() throws IOException {
    Path testParent = Files.createTempDirectory("shouldListDirectories");
    Files.createFile(
        Path.of(Files.createDirectories(Path.of(testParent + "/foo/bar")) + "/a-list.txt"));
    Files.createFile(
        Path.of(Files.createDirectories(Path.of(testParent + "/foo/baz")) + "/b-list.txt"));
    Files.createFile(
        Path.of(Files.createDirectories(Path.of(testParent + "/baz")) + "/c-list.txt"));
    Files.createFile(
        Path.of(Files.createDirectories(Path.of(testParent + "/baz/bar")) + "/d-list.txt"));
    Files.createFile(Path.of(testParent + "/e-list.txt"));

    LocalBlobFs localBlobFs = new LocalBlobFs();

    assertThat(localBlobFs.listDirectories(testParent.toUri()))
        .containsExactlyInAnyOrder("foo", "baz");
    assertThat(localBlobFs.listDirectories(URI.create(testParent + "/foo")))
        .containsExactlyInAnyOrder("bar", "baz");
    assertThat(localBlobFs.listDirectories(URI.create(testParent + "/foo/bar"))).isEmpty();
    assertThat(localBlobFs.listDirectories(URI.create(testParent + "/baz")))
        .containsExactlyInAnyOrder("bar");

    Throwable nonExistingPath =
        catchThrowable(() -> localBlobFs.listDirectories(URI.create(testParent + "/quz")));
    assertThat(nonExistingPath).isInstanceOf(IOException.class);
    Throwable notAPath =
        catchThrowable(() -> localBlobFs.listDirectories(URI.create(testParent + "/e-list.txt")));
    assertThat(notAPath).isInstanceOf(IOException.class);
  }

  @Test
  public void testFS() throws Exception {
    LocalBlobFs localBlobFs = new LocalBlobFs();
    URI testFileUri = testFile.toURI();
    // Check whether a directory exists
    Assert.assertTrue(localBlobFs.exists(absoluteTmpDirPath.toURI()));
    Assert.assertTrue(localBlobFs.lastModified(absoluteTmpDirPath.toURI()) > 0L);
    Assert.assertTrue(localBlobFs.isDirectory(absoluteTmpDirPath.toURI()));
    // Check whether a file exists
    Assert.assertTrue(localBlobFs.exists(testFileUri));
    Assert.assertFalse(localBlobFs.isDirectory(testFileUri));

    File file = new File(absoluteTmpDirPath, "secondTestFile");
    URI secondTestFileUri = file.toURI();
    // Check that file does not exist
    Assert.assertTrue(!localBlobFs.exists(secondTestFileUri));

    localBlobFs.copy(testFileUri, secondTestFileUri);
    Assert.assertEquals(2, localBlobFs.listFiles(absoluteTmpDirPath.toURI(), true).length);

    // Check file copy worked when file was not created
    Assert.assertTrue(localBlobFs.exists(secondTestFileUri));

    // Create another file in the same path
    File thirdTestFile = new File(absoluteTmpDirPath, "thirdTestFile");
    Assert.assertTrue(
        "Could not create file " + thirdTestFile.getPath(), thirdTestFile.createNewFile());

    File newAbsoluteTempDirPath = new File(absoluteTmpDirPath, "absoluteTwo");
    Assert.assertTrue(newAbsoluteTempDirPath.mkdir());

    // Create a testDir and file underneath directory
    File testDir = new File(newAbsoluteTempDirPath, "testDir");
    Assert.assertTrue("Could not make directory " + testDir.getAbsolutePath(), testDir.mkdir());
    File testDirFile = new File(testDir, "testFile");
    // Assert that recursive list files and nonrecursive list files are as expected
    Assert.assertTrue(
        "Could not create file " + testDir.getAbsolutePath(), testDirFile.createNewFile());
    Assert.assertEquals(
        localBlobFs.listFiles(newAbsoluteTempDirPath.toURI(), false),
        new String[] {testDir.getAbsolutePath()});
    Assert.assertEquals(
        localBlobFs.listFiles(newAbsoluteTempDirPath.toURI(), true),
        new String[] {testDir.getAbsolutePath(), testDirFile.getAbsolutePath()});

    // Create another parent dir so we can test recursive move
    File newAbsoluteTempDirPath3 = new File(absoluteTmpDirPath, "absoluteThree");
    Assert.assertTrue(newAbsoluteTempDirPath3.mkdir());
    Assert.assertEquals(newAbsoluteTempDirPath3.listFiles().length, 0);

    localBlobFs.move(newAbsoluteTempDirPath.toURI(), newAbsoluteTempDirPath3.toURI(), true);
    Assert.assertFalse(localBlobFs.exists(newAbsoluteTempDirPath.toURI()));
    Assert.assertTrue(localBlobFs.exists(newAbsoluteTempDirPath3.toURI()));
    Assert.assertTrue(localBlobFs.exists(new File(newAbsoluteTempDirPath3, "testDir").toURI()));
    Assert.assertTrue(
        localBlobFs.exists(
            new File(new File(newAbsoluteTempDirPath3, "testDir"), "testFile").toURI()));

    // Check if using a different scheme on URI still works
    URI uri = URI.create("hdfs://localhost:9999" + newAbsoluteTempDirPath.getPath());
    localBlobFs.move(newAbsoluteTempDirPath3.toURI(), uri, true);
    Assert.assertFalse(localBlobFs.exists(newAbsoluteTempDirPath3.toURI()));
    Assert.assertTrue(localBlobFs.exists(newAbsoluteTempDirPath.toURI()));
    Assert.assertTrue(localBlobFs.exists(new File(newAbsoluteTempDirPath, "testDir").toURI()));
    Assert.assertTrue(
        localBlobFs.exists(
            new File(new File(newAbsoluteTempDirPath, "testDir"), "testFile").toURI()));

    // Check file copy to location where something already exists still works
    localBlobFs.copy(testFileUri, thirdTestFile.toURI());
    // Check length of file
    Assert.assertEquals(0, localBlobFs.length(secondTestFileUri));
    Assert.assertTrue(localBlobFs.exists(thirdTestFile.toURI()));

    // Check that method deletes dst directory during move and is successful by overwriting dir
    Assert.assertTrue(newTmpDir.exists());
    // create a file in the dst folder
    File dstFile = new File(newTmpDir.getPath() + "/newFile");
    dstFile.createNewFile();

    // Expected that a move without overwrite will not succeed
    Assert.assertFalse(localBlobFs.move(absoluteTmpDirPath.toURI(), newTmpDir.toURI(), false));

    int files = absoluteTmpDirPath.listFiles().length;
    Assert.assertTrue(localBlobFs.move(absoluteTmpDirPath.toURI(), newTmpDir.toURI(), true));
    Assert.assertEquals(absoluteTmpDirPath.length(), 0);
    Assert.assertEquals(newTmpDir.listFiles().length, files);
    Assert.assertFalse(dstFile.exists());

    // Check that a moving a file to a non-existent destination folder will work
    FileUtils.deleteQuietly(nonExistentTmpFolder);
    Assert.assertFalse(nonExistentTmpFolder.exists());
    File srcFile = new File(absoluteTmpDirPath, "srcFile");
    localBlobFs.mkdir(absoluteTmpDirPath.toURI());
    Assert.assertTrue(srcFile.createNewFile());
    dstFile = new File(nonExistentTmpFolder.getPath() + "/newFile");
    Assert.assertFalse(dstFile.exists());
    Assert.assertTrue(
        localBlobFs.move(srcFile.toURI(), dstFile.toURI(), true)); // overwrite flag has no impact
    Assert.assertFalse(srcFile.exists());
    Assert.assertTrue(dstFile.exists());

    // Check that moving a folder to a non-existent destination folder works
    FileUtils.deleteQuietly(nonExistentTmpFolder);
    Assert.assertFalse(nonExistentTmpFolder.exists());
    srcFile = new File(absoluteTmpDirPath, "srcFile");
    localBlobFs.mkdir(absoluteTmpDirPath.toURI());
    Assert.assertTrue(srcFile.createNewFile());
    dstFile = new File(nonExistentTmpFolder.getPath() + "/srcFile");
    Assert.assertFalse(dstFile.exists());
    Assert.assertTrue(
        localBlobFs.move(
            absoluteTmpDirPath.toURI(),
            nonExistentTmpFolder.toURI(),
            true)); // overwrite flag has no impact
    Assert.assertTrue(dstFile.exists());

    localBlobFs.delete(secondTestFileUri, true);
    // Check deletion from final location worked
    Assert.assertTrue(!localBlobFs.exists(secondTestFileUri));

    File firstTempDir = new File(absoluteTmpDirPath, "firstTempDir");
    File secondTempDir = new File(absoluteTmpDirPath, "secondTempDir");
    localBlobFs.mkdir(firstTempDir.toURI());
    Assert.assertTrue("Could not make directory " + firstTempDir.getPath(), firstTempDir.exists());

    // Check that touching a file works
    File nonExistingFile = new File(absoluteTmpDirPath, "nonExistingFile");
    Assert.assertFalse(nonExistingFile.exists());
    localBlobFs.touch(nonExistingFile.toURI());
    Assert.assertTrue(nonExistingFile.exists());
    long currentTime = System.currentTimeMillis();
    Assert.assertTrue(localBlobFs.lastModified(nonExistingFile.toURI()) <= currentTime);
    Thread.sleep(1000L);
    // update last modified.
    localBlobFs.touch(nonExistingFile.toURI());
    Assert.assertTrue(localBlobFs.lastModified(nonExistingFile.toURI()) > currentTime);
    FileUtils.deleteQuietly(nonExistingFile);

    // Check that touch an file in a directory that doesn't exist should throw an exception.
    File nonExistingFileUnderNonExistingDir =
        new File(absoluteTmpDirPath, "nonExistingDir/nonExistingFile");
    Assert.assertFalse(nonExistingFileUnderNonExistingDir.exists());
    try {
      localBlobFs.touch(nonExistingFileUnderNonExistingDir.toURI());
      fail("Touch method should throw an IOException");
    } catch (IOException e) {
      // Expected.
    }

    // Check that directory only copy worked
    localBlobFs.copy(firstTempDir.toURI(), secondTempDir.toURI());
    Assert.assertTrue(localBlobFs.exists(secondTempDir.toURI()));

    // Copying directory with files to directory with files
    File testFile = new File(firstTempDir, "testFile");
    Assert.assertTrue("Could not create file " + testFile.getPath(), testFile.createNewFile());
    File newTestFile = new File(secondTempDir, "newTestFile");
    Assert.assertTrue(
        "Could not create file " + newTestFile.getPath(), newTestFile.createNewFile());

    localBlobFs.copy(firstTempDir.toURI(), secondTempDir.toURI());
    Assert.assertEquals(localBlobFs.listFiles(secondTempDir.toURI(), true).length, 1);

    // Copying directory with files under another directory.
    File firstTempDirUnderSecondTempDir = new File(secondTempDir, firstTempDir.getName());
    localBlobFs.copy(firstTempDir.toURI(), firstTempDirUnderSecondTempDir.toURI());
    Assert.assertTrue(localBlobFs.exists(firstTempDirUnderSecondTempDir.toURI()));
    // There're two files/directories under secondTempDir.
    Assert.assertEquals(localBlobFs.listFiles(secondTempDir.toURI(), false).length, 2);
    // The file under src directory also got copied under dst directory.
    Assert.assertEquals(
        localBlobFs.listFiles(firstTempDirUnderSecondTempDir.toURI(), true).length, 1);

    // len of dir = exception
    try {
      localBlobFs.length(firstTempDir.toURI());
      fail();
    } catch (IllegalArgumentException e) {

    }

    Assert.assertTrue(testFile.exists());

    localBlobFs.copyFromLocalFile(testFile, secondTestFileUri);
    Assert.assertTrue(localBlobFs.exists(secondTestFileUri));
    localBlobFs.copyToLocalFile(testFile.toURI(), new File(secondTestFileUri));
    Assert.assertTrue(localBlobFs.exists(secondTestFileUri));
  }
}
