/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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

import java.io.File;
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
        testCachedPopulated(StandardOpenOption.READ,"temp4", false);
    }

    private void testCachedPopulated(StandardOpenOption option, String fileName, boolean isWrite) throws IOException {
        //Arrange.
        Path filePath = tempDirPath.resolve(Path.of(fileName));
        FileSystemWrapper fw = new FileSystemWrapper();
        //Make sure file doesn't exist
        Assert.assertFalse(fw.exists(filePath));
        Assert.assertNull(fw.getWriteCache().getIfPresent(filePath.toString()));
        Assert.assertNull(fw.getReadCache().getIfPresent(filePath.toString()));
        //Act
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);
        fw.createFile(fileAttributes, filePath);
        //Assert
        Assert.assertTrue(fw.exists(filePath));
        FileChannel fileChannel = fw.getFileChannel(filePath, option);
        Assert.assertNotNull(fileChannel);
        Assert.assertEquals(0,fileChannel.size());
        FileChannel fileChannelFromCache;
        if(isWrite){
            fileChannelFromCache = (fw.getWriteCache().getIfPresent(filePath.toString()));
        } else {
            fileChannelFromCache = (fw.getReadCache().getIfPresent(filePath.toString()));
        }
        Assert.assertNotNull(fileChannelFromCache);
        Assert.assertEquals(fileChannel, fileChannelFromCache);
        if(isWrite){
            Assert.assertNull(fw.getReadCache().getIfPresent(filePath.toString()));
        }else {
            Assert.assertNull(fw.getWriteCache().getIfPresent(filePath.toString()));
        }
    }

    @Test
    public void validateIsWriteable() throws IOException {
        String fileName1 = "temp5";
        Path filePath = tempDirPath.resolve(Path.of(fileName1));
        FileSystemWrapper fw = new FileSystemWrapper();
        //Checking file exists = true
        //Assert.assertTrue(fw.exists(filePath));
        //If file doesn't exist then create the file
        Assert.assertFalse(fw.exists(filePath));
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);
        fw.createFile(fileAttributes, filePath);
        //Act
        Assert.assertTrue(fw.exists(filePath));
        FileChannel fileChannel = fw.getFileChannel(filePath, StandardOpenOption.WRITE);
        FileChannel fileChannelFromCache = (fw.getWriteCache().getIfPresent(filePath.toString()));
        Assert.assertTrue(fw.isWritable(filePath));
        }

        @Test
    public void validateIsRegularFile(){

        }
    }

