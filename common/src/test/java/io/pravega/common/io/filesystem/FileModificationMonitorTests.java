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
package io.pravega.common.io.filesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

public abstract class FileModificationMonitorTests {

    final static Path PATH_VALID_NONEXISTENT = Paths.get(File.pathSeparator +
            System.currentTimeMillis() + File.pathSeparator + System.currentTimeMillis());

    private final static Path PATH_EMPTY = Paths.get("");
    private final static Path PATH_NONEMPTY = Paths.get("non-empty");
    private final static Path PATH_NONEXISTENT = Paths.get(System.currentTimeMillis() + ".file");

    abstract FileModificationMonitor prepareObjectUnderTest(Path path) throws FileNotFoundException;

    abstract FileModificationMonitor prepareObjectUnderTest(Path path, boolean checkForFileExistence)
            throws FileNotFoundException;

    @Test(expected = NullPointerException.class)
    public void testCtorRejectsNullFilePathInput() throws FileNotFoundException {
        prepareObjectUnderTest(null);
    }

    @Test(expected = NullPointerException.class)
    public void testCtorRejectsNullConsumer() throws FileNotFoundException {
        new FileModificationEventWatcher(PATH_NONEMPTY, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorRejectsEmptyFileArgument() throws FileNotFoundException {
        prepareObjectUnderTest(PATH_EMPTY);
    }

    @Test(expected = FileNotFoundException.class)
    public void testCtorRejectsNonExistentFileArgument() throws FileNotFoundException {
        prepareObjectUnderTest(PATH_NONEXISTENT, true);
    }

    @Test
    public void testStopWithNoStartCompletesGracefully() throws IOException {
        FileModificationMonitor monitor = prepareObjectUnderTest(PATH_VALID_NONEXISTENT, false);
        monitor.stopMonitoring();
    }

    Path prepareRootDirPath() {
        // This is a trick to avoid SpotBugs error: DMI_HARDCODED_ABSOLUTE_FILENAME
        String root = "/";
        return Paths.get(root + ""); // "/" in Linux and "\" in Windows
    }

    static File createTempFile() throws IOException {
        Path dir = Files.createTempDirectory("fw-");
        return File.createTempFile("tf-", ".temp", dir.toFile());
    }

    static void cleanupTempFile(File file) throws IOException {
        Preconditions.checkNotNull(file);
        if (file.toPath() == null) {
            return;
        }
        Path dirPath = file.toPath().getParent();

        if (file.exists()) {
            file.delete();
        }
        if (dirPath != null) {
            FileUtils.deleteDirectory(dirPath.toFile());
        }
    }
}
