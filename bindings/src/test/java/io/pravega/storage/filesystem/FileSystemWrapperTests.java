/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.storage.filesystem;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public class FileSystemWrapperTests {
    public static final int CACHE_SIZE = 1024;
    public static final String USER_NAME_PROPERTY = "user.name";
    public static final String ROOT_USER_NAME = "root";
    Path tempDirPath;

    @Before
    public void setUp() throws IOException {
        tempDirPath = Files.createTempDirectory("xyz");
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(tempDirPath.toFile());
    }

    @Test
    public void testCachePopulated() throws IOException {
        testCachedPopulated(StandardOpenOption.WRITE, "temp1", true);
        testCachedPopulated(StandardOpenOption.CREATE_NEW, "temp2", true);
        testCachedPopulated(StandardOpenOption.CREATE, "temp3", true);
        testCachedPopulated(StandardOpenOption.READ, "temp4", false);
    }

    private void testCachedPopulated(StandardOpenOption option, String fileName, boolean isWrite) throws IOException {
        // Arrange. Create FileSystemWrapper instance to test
        Path filePath = tempDirPath.resolve(Path.of(fileName));
        FileSystemWrapper fw = new FileSystemWrapper(CACHE_SIZE, CACHE_SIZE);

        // Check that file doesn't exist
        Assert.assertFalse(fw.exists(filePath));

        // Check that key doesn't exist in cache
        Assert.assertNull(fw.getWriteCache().getIfPresent(filePath.toString()));
        Assert.assertNull(fw.getReadCache().getIfPresent(filePath.toString()));

        // Act. Create file
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);
        fw.createFile(fileAttributes, filePath);

        // Assert. Check that file now exists
        Assert.assertTrue(fw.exists(filePath));

        // Open a fileChannel
        FileChannel fileChannel = fw.getFileChannel(filePath, option);
        Assert.assertNotNull(fileChannel);
        Assert.assertEquals(0, fileChannel.size());

        // Check fileChannel instance exists in appropriate cache
        FileChannel fileChannelFromCache;
        if (isWrite) {
            fileChannelFromCache = fw.getWriteCache().getIfPresent(filePath.toString());
        } else {
            fileChannelFromCache = fw.getReadCache().getIfPresent(filePath.toString());
        }
        Assert.assertNotNull(fileChannelFromCache);

        if (isWrite) {
            Assert.assertNull(fw.getReadCache().getIfPresent(filePath.toString()));
        } else {
            Assert.assertNull(fw.getWriteCache().getIfPresent(filePath.toString()));
        }

        // Check that we get the same instance that is put in the cache
        Assert.assertEquals(fileChannel, fileChannelFromCache);
    }

    @Test
    public void validateIsWriteable() throws IOException {
        // Arrange. Create FileSystemWrapper instance to test
        String fileName1 = "temp5";
        Path filePath = tempDirPath.resolve(Path.of(fileName1));
        FileSystemWrapper fw = new FileSystemWrapper(CACHE_SIZE, CACHE_SIZE);

        // Check that the file doesn't exist
        Assert.assertFalse(fw.exists(filePath));

        // Act. Create the file
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);
        fw.createFile(fileAttributes, filePath);

        // Assert. Check that file now exists
        Assert.assertTrue(fw.exists(filePath));

        // Open fileChannel
        FileChannel fileChannel = fw.getFileChannel(filePath, StandardOpenOption.WRITE);
        FileChannel fileChannelFromCache = fw.getWriteCache().getIfPresent(filePath.toString());

        // Check the file isWritable
        Assert.assertTrue(fw.isWritable(filePath));

        // Check that we get the same instance that is put in the cache
        Assert.assertEquals(fileChannel, fileChannelFromCache);
    }

    @Test
    public void verifyDelete() throws IOException {
        // Arrange. Create FileSystemWrapper instance to test
        String fileName2 = "temp6";
        Path filePath = tempDirPath.resolve(Path.of(fileName2));
        FileSystemWrapper fw = new FileSystemWrapper(CACHE_SIZE, CACHE_SIZE);

        // Check that file doesn't exist
        boolean b = fw.exists(filePath);
        Assert.assertFalse(b);

        // Act. Create the file
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);
        fw.createFile(fileAttributes, filePath);

        // Assert. Check that file now exists
        Assert.assertTrue(fw.exists(filePath));

        // verifyDelete by deleting the filePath and checking if the file present.
        fw.delete(filePath);
        Assert.assertFalse(fw.exists(filePath));
        Assert.assertNull(fw.getWriteCache().getIfPresent(fileName2));
    }

    @Test
    public void validateIsRegularFile() throws IOException {
        // Arrange. Create FileSystemWrapper instance to test
        String fileName3 = "temp7";
        Path filePath = tempDirPath.resolve(Path.of(fileName3));
        FileSystemWrapper fw = new FileSystemWrapper(CACHE_SIZE, CACHE_SIZE);

        // Check that file doesn't exist
        Assert.assertFalse(fw.exists(filePath));

        // Act. Create the file
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);
        fw.createFile(fileAttributes, filePath);

        // Open fileChannel
        FileChannel fileChannel = fw.getFileChannel(filePath, StandardOpenOption.WRITE);

        // Checking if the file is a Regular file
        Assert.assertNotNull(fileChannel);
        Assert.assertTrue(fw.isRegularFile(filePath));
    }

    @Test
    public void validateSetPermissions() throws IOException {
        if (isRootUser()) {
            return;
        }
        // Arrange. Create FileSystemWrapper instance to test
        String filename4 = "temp8";
        Path filePath = tempDirPath.resolve(Path.of(filename4));
        FileSystemWrapper fw = new FileSystemWrapper(CACHE_SIZE, CACHE_SIZE);

        // Check that file doesn't exist
        Assert.assertFalse(fw.exists(filePath));

        // Act. Create the file
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_ONLY_PERMISSION);
        fw.createFile(fileAttributes, filePath);

        // Setting Read only permission on the file and verifying that the file is not writable
        Assert.assertTrue(fw.exists(filePath));
        fw.setPermissions(filePath, FileSystemWrapper.READ_ONLY_PERMISSION);
        Assert.assertFalse(fw.isWritable(filePath));
    }

    @Test
    public void checkGetFileSize() throws IOException {
        // Arrange. Create FileSystemWrapper instance to test
        String filename5 = "temp9";
        Path filePath = tempDirPath.resolve(Path.of(filename5));
        FileSystemWrapper fw = new FileSystemWrapper(CACHE_SIZE, CACHE_SIZE);

        // Check that file doesn't exist
        Assert.assertFalse(fw.exists(filePath));

        // Act. Create the file
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);
        fw.createFile(fileAttributes, filePath);

        // Assert. Check that file now exists
        Assert.assertTrue(fw.exists(filePath));

        // Checking that the size of the file is 0 as the file doesn't have any content within
        Assert.assertEquals(0, fw.getFileSize(filePath));
    }

    @Test
    public void testIsRegularFileForDirectory() throws IOException {
        // Arrange. Create FileSystemWrapper instance to test
        String dirName = "tempDir1";
        Path dirPath = tempDirPath.resolve(Path.of(dirName));
        FileSystemWrapper fw = new FileSystemWrapper(CACHE_SIZE, CACHE_SIZE);

        // Check that directory doesn't exist
        Assert.assertFalse(fw.exists(dirPath));

        // Act. Create new directory
        fw.createDirectories(dirPath);

        // Assert. Check that the directory exists now
        Assert.assertTrue(fw.exists(dirPath));

        // Verify that the directory created is not a regular file
        Assert.assertFalse(fw.isRegularFile(dirPath));
    }

    @Test
    public void validateSetPermissionsAfter() throws IOException {
        if (isRootUser()) {
            return;
        }
        // Arrange. Create FileSystemWrapper instance to test
        String filename6 = "temp10";
        Path filePath = tempDirPath.resolve(Path.of(filename6));
        FileSystemWrapper fw = new FileSystemWrapper(CACHE_SIZE, CACHE_SIZE);

        // Check that file doesn't exist
        Assert.assertFalse(fw.exists(filePath));

        // Act. Create the file
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);
        fw.createFile(fileAttributes, filePath);

        // Assert. Check that the file isWriteable
        Assert.assertTrue(fw.isWritable(filePath));

        // Assert. Check that the file exists
        Assert.assertTrue(fw.exists(filePath));

        // Set read only permission on the file and verify that it's not writable
        fw.setPermissions(filePath, FileSystemWrapper.READ_ONLY_PERMISSION);
        Assert.assertFalse(fw.isWritable(filePath));

        // Set read-write permission on the file and verify that it's writable
        fw.setPermissions(filePath, FileSystemWrapper.READ_WRITE_PERMISSION);
        Assert.assertTrue(fw.isWritable(filePath));
    }

    /**
     * All files are always writeable for root, thus making test irrelevant
     */
    private boolean isRootUser() {
        return System.getProperty(USER_NAME_PROPERTY).equals(ROOT_USER_NAME);
    }
}

