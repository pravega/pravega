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
package io.pravega.cli.admin.utils;

import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.pravega.cli.admin.utils.FileHelper.createFileAndDirectory;

public class FileHelperTest {

    @Test
    public void testFileCreationSuccess() throws IOException {
        Path tempDirPath = Files.createTempDirectory("fileHelperDir");
        String filename = Paths.get(tempDirPath.toString(), "tmp" + System.currentTimeMillis(), "fileHelperTest.txt").toString();
        createFileAndDirectory(filename);
        File file = new File(filename);
        Assert.assertTrue(file.exists());
        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));
        // Delete the temporary directory.
        tempDirPath.toFile().deleteOnExit();
    }

    @Test
    public void testFileCreationSuccessNoParentCase() throws IOException {
        Path tempDirPath = Files.createTempDirectory("fileHelperDir");
        String filename = Paths.get(tempDirPath.toString(), "fileHelperTest.txt").toString();
        createFileAndDirectory(filename);
        File file = new File(filename);
        Assert.assertTrue(file.exists());
        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));
        // Delete the temporary directory.
        tempDirPath.toFile().deleteOnExit();
    }

    @Test
    public void testFileCreationFailureFileAlreadyExists() throws IOException {
        Path tempDirPath = Files.createTempDirectory("fileHelperFailDir");
        String filename = Paths.get(tempDirPath.toString(), "tmp" + System.currentTimeMillis(), "fileHelperTest.txt").toString();
        createFileAndDirectory(filename);
        File file = new File(filename);
        Assert.assertTrue(file.exists());
        AssertExtensions.assertThrows(FileAlreadyExistsException.class, () -> createFileAndDirectory(filename));
        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));
        // Delete the temporary directory.
        tempDirPath.toFile().deleteOnExit();
    }
}
