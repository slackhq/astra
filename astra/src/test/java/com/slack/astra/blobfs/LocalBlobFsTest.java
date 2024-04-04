package com.slack.astra.blobfs;

import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LocalBlobFsTest {
  private File testFile;
  private File absoluteTmpDirPath;
  private File newTmpDir;
  private File nonExistentTmpFolder;

  @BeforeEach
  public void setUp() {
    absoluteTmpDirPath =
        new File(
            System.getProperty("java.io.tmpdir"), LocalBlobFsTest.class.getSimpleName() + "first");
    FileUtils.deleteQuietly(absoluteTmpDirPath);
    assertTrue(
        absoluteTmpDirPath.mkdir(), "Could not make directory " + absoluteTmpDirPath.getPath());
    try {
      testFile = new File(absoluteTmpDirPath, "testFile");
      assertTrue(testFile.createNewFile(), "Could not create file " + testFile.getPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    newTmpDir =
        new File(
            System.getProperty("java.io.tmpdir"), LocalBlobFsTest.class.getSimpleName() + "second");
    FileUtils.deleteQuietly(newTmpDir);
    assertTrue(newTmpDir.mkdir(), "Could not make directory " + newTmpDir.getPath());

    nonExistentTmpFolder =
        new File(
            System.getProperty("java.io.tmpdir"),
            LocalBlobFsTest.class.getSimpleName() + "nonExistentParent/nonExistent");

    absoluteTmpDirPath.deleteOnExit();
    newTmpDir.deleteOnExit();
    nonExistentTmpFolder.deleteOnExit();
  }

  @AfterEach
  public void tearDown() {
    absoluteTmpDirPath.delete();
    newTmpDir.delete();
  }

  @Test
  public void testFS() throws Exception {
    LocalBlobFs localBlobFs = new LocalBlobFs();
    URI testFileUri = testFile.toURI();
    // Check whether a directory exists
    assertTrue(localBlobFs.exists(absoluteTmpDirPath.toURI()));
    assertTrue(localBlobFs.lastModified(absoluteTmpDirPath.toURI()) > 0L);
    assertTrue(localBlobFs.isDirectory(absoluteTmpDirPath.toURI()));
    // Check whether a file exists
    assertTrue(localBlobFs.exists(testFileUri));
    assertFalse(localBlobFs.isDirectory(testFileUri));

    File file = new File(absoluteTmpDirPath, "secondTestFile");
    URI secondTestFileUri = file.toURI();
    // Check that file does not exist
    assertTrue(!localBlobFs.exists(secondTestFileUri));

    localBlobFs.copy(testFileUri, secondTestFileUri);
    assertEquals(2, localBlobFs.listFiles(absoluteTmpDirPath.toURI(), true).length);

    // Check file copy worked when file was not created
    assertTrue(localBlobFs.exists(secondTestFileUri));

    // Create another file in the same path
    File thirdTestFile = new File(absoluteTmpDirPath, "thirdTestFile");
    assertTrue(thirdTestFile.createNewFile(), "Could not create file " + thirdTestFile.getPath());

    File newAbsoluteTempDirPath = new File(absoluteTmpDirPath, "absoluteTwo");
    assertTrue(newAbsoluteTempDirPath.mkdir());

    // Create a testDir and file underneath directory
    File testDir = new File(newAbsoluteTempDirPath, "testDir");
    assertTrue(testDir.mkdir(), "Could not make directory " + testDir.getAbsolutePath());
    File testDirFile = new File(testDir, "testFile");
    // Assert that recursive list files and nonrecursive list files are as expected
    assertTrue(testDirFile.createNewFile(), "Could not create file " + testDir.getAbsolutePath());
    assertArrayEquals(
        localBlobFs.listFiles(newAbsoluteTempDirPath.toURI(), false),
        new String[] {testDir.getAbsolutePath()});
    assertArrayEquals(
        localBlobFs.listFiles(newAbsoluteTempDirPath.toURI(), true),
        new String[] {testDir.getAbsolutePath(), testDirFile.getAbsolutePath()});

    // Create another parent dir so we can test recursive move
    File newAbsoluteTempDirPath3 = new File(absoluteTmpDirPath, "absoluteThree");
    assertTrue(newAbsoluteTempDirPath3.mkdir());
    assertEquals(newAbsoluteTempDirPath3.listFiles().length, 0);

    localBlobFs.move(newAbsoluteTempDirPath.toURI(), newAbsoluteTempDirPath3.toURI(), true);
    assertFalse(localBlobFs.exists(newAbsoluteTempDirPath.toURI()));
    assertTrue(localBlobFs.exists(newAbsoluteTempDirPath3.toURI()));
    assertTrue(localBlobFs.exists(new File(newAbsoluteTempDirPath3, "testDir").toURI()));
    assertTrue(
        localBlobFs.exists(
            new File(new File(newAbsoluteTempDirPath3, "testDir"), "testFile").toURI()));

    // Check if using a different scheme on URI still works
    URI uri = URI.create("hdfs://localhost:9999" + newAbsoluteTempDirPath.getPath());
    localBlobFs.move(newAbsoluteTempDirPath3.toURI(), uri, true);
    assertFalse(localBlobFs.exists(newAbsoluteTempDirPath3.toURI()));
    assertTrue(localBlobFs.exists(newAbsoluteTempDirPath.toURI()));
    assertTrue(localBlobFs.exists(new File(newAbsoluteTempDirPath, "testDir").toURI()));
    assertTrue(
        localBlobFs.exists(
            new File(new File(newAbsoluteTempDirPath, "testDir"), "testFile").toURI()));

    // Check file copy to location where something already exists still works
    localBlobFs.copy(testFileUri, thirdTestFile.toURI());
    // Check length of file
    assertEquals(0, localBlobFs.length(secondTestFileUri));
    assertTrue(localBlobFs.exists(thirdTestFile.toURI()));

    // Check that method deletes dst directory during move and is successful by overwriting dir
    assertTrue(newTmpDir.exists());
    // create a file in the dst folder
    File dstFile = new File(newTmpDir.getPath() + "/newFile");
    dstFile.createNewFile();

    // Expected that a move without overwrite will not succeed
    assertFalse(localBlobFs.move(absoluteTmpDirPath.toURI(), newTmpDir.toURI(), false));

    int files = absoluteTmpDirPath.listFiles().length;
    assertTrue(localBlobFs.move(absoluteTmpDirPath.toURI(), newTmpDir.toURI(), true));
    assertEquals(absoluteTmpDirPath.length(), 0);
    assertEquals(newTmpDir.listFiles().length, files);
    assertFalse(dstFile.exists());

    // Check that a moving a file to a non-existent destination folder will work
    FileUtils.deleteQuietly(nonExistentTmpFolder);
    assertFalse(nonExistentTmpFolder.exists());
    File srcFile = new File(absoluteTmpDirPath, "srcFile");
    localBlobFs.mkdir(absoluteTmpDirPath.toURI());
    assertTrue(srcFile.createNewFile());
    dstFile = new File(nonExistentTmpFolder.getPath() + "/newFile");
    assertFalse(dstFile.exists());
    assertTrue(
        localBlobFs.move(srcFile.toURI(), dstFile.toURI(), true)); // overwrite flag has no impact
    assertFalse(srcFile.exists());
    assertTrue(dstFile.exists());

    // Check that moving a folder to a non-existent destination folder works
    FileUtils.deleteQuietly(nonExistentTmpFolder);
    assertFalse(nonExistentTmpFolder.exists());
    srcFile = new File(absoluteTmpDirPath, "srcFile");
    localBlobFs.mkdir(absoluteTmpDirPath.toURI());
    assertTrue(srcFile.createNewFile());
    dstFile = new File(nonExistentTmpFolder.getPath() + "/srcFile");
    assertFalse(dstFile.exists());
    assertTrue(
        localBlobFs.move(
            absoluteTmpDirPath.toURI(),
            nonExistentTmpFolder.toURI(),
            true)); // overwrite flag has no impact
    assertTrue(dstFile.exists());

    localBlobFs.delete(secondTestFileUri, true);
    // Check deletion from final location worked
    assertTrue(!localBlobFs.exists(secondTestFileUri));

    File firstTempDir = new File(absoluteTmpDirPath, "firstTempDir");
    File secondTempDir = new File(absoluteTmpDirPath, "secondTempDir");
    localBlobFs.mkdir(firstTempDir.toURI());
    assertTrue(firstTempDir.exists(), "Could not make directory " + firstTempDir.getPath());

    // Check that touching a file works
    File nonExistingFile = new File(absoluteTmpDirPath, "nonExistingFile");
    assertFalse(nonExistingFile.exists());
    localBlobFs.touch(nonExistingFile.toURI());
    assertTrue(nonExistingFile.exists());
    long currentTime = System.currentTimeMillis();
    assertTrue(localBlobFs.lastModified(nonExistingFile.toURI()) <= currentTime);
    Thread.sleep(1L);
    // update last modified.
    localBlobFs.touch(nonExistingFile.toURI());
    assertTrue(localBlobFs.lastModified(nonExistingFile.toURI()) > currentTime);
    FileUtils.deleteQuietly(nonExistingFile);

    // Check that touch an file in a directory that doesn't exist should throw an exception.
    File nonExistingFileUnderNonExistingDir =
        new File(absoluteTmpDirPath, "nonExistingDir/nonExistingFile");
    assertFalse(nonExistingFileUnderNonExistingDir.exists());
    try {
      localBlobFs.touch(nonExistingFileUnderNonExistingDir.toURI());
      fail("Touch method should throw an IOException");
    } catch (IOException e) {
      // Expected.
    }

    // Check that directory only copy worked
    localBlobFs.copy(firstTempDir.toURI(), secondTempDir.toURI());
    assertTrue(localBlobFs.exists(secondTempDir.toURI()));

    // Copying directory with files to directory with files
    File testFile = new File(firstTempDir, "testFile");
    assertTrue(testFile.createNewFile(), "Could not create file " + testFile.getPath());
    File newTestFile = new File(secondTempDir, "newTestFile");
    assertTrue(newTestFile.createNewFile(), "Could not create file " + newTestFile.getPath());

    localBlobFs.copy(firstTempDir.toURI(), secondTempDir.toURI());
    assertEquals(localBlobFs.listFiles(secondTempDir.toURI(), true).length, 1);

    // Copying directory with files under another directory.
    File firstTempDirUnderSecondTempDir = new File(secondTempDir, firstTempDir.getName());
    localBlobFs.copy(firstTempDir.toURI(), firstTempDirUnderSecondTempDir.toURI());
    assertTrue(localBlobFs.exists(firstTempDirUnderSecondTempDir.toURI()));
    // There're two files/directories under secondTempDir.
    assertEquals(localBlobFs.listFiles(secondTempDir.toURI(), false).length, 2);
    // The file under src directory also got copied under dst directory.
    assertEquals(localBlobFs.listFiles(firstTempDirUnderSecondTempDir.toURI(), true).length, 1);

    // len of dir = exception
    try {
      localBlobFs.length(firstTempDir.toURI());
      fail("Exception expected that did not occur");
    } catch (IllegalArgumentException e) {

    }

    assertTrue(testFile.exists());

    localBlobFs.copyFromLocalFile(testFile, secondTestFileUri);
    assertTrue(localBlobFs.exists(secondTestFileUri));
    localBlobFs.copyToLocalFile(testFile.toURI(), new File(secondTestFileUri));
    assertTrue(localBlobFs.exists(secondTestFileUri));
  }
}
