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

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileModificationEventWatcherTests extends FileModificationMonitorTests {

    /**
     * Holds a file created for shared use of tests. The lifecycle of this file is managed in this class. No tests
     * should write to this file or delete this file.
     */
    private final static File SHARED_FILE;

    private final static Consumer<WatchEvent<?>> NOOP_CONSUMER = c -> { };

    static {
        try {
            SHARED_FILE = createTempFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void cleanup() {
        try {
            cleanupTempFile(SHARED_FILE);
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    FileModificationMonitor prepareObjectUnderTest(Path path) throws FileNotFoundException {
        return prepareObjectUnderTest(path, true);
    }

    @Override
    FileModificationMonitor prepareObjectUnderTest(Path path, boolean checkForFileExistence) throws FileNotFoundException {
        return new FileModificationEventWatcher(path, NOOP_CONSUMER, true, checkForFileExistence);
    }

    @Test(timeout = 15000)
    public void testInvokesCallBackForFileModification() throws IOException, InterruptedException {
        File tempFile = FileModificationMonitorTests.createTempFile();

        AtomicBoolean isCallbackInvoked = new AtomicBoolean(false);
        FileModificationEventWatcher watcher = new FileModificationEventWatcher(tempFile.toPath(),
                c -> isCallbackInvoked.set(true), false, true);
        watcher.startMonitoring();

        // The watcher might not get fully started when we modify the watched file, so ensuring that it is.
        this.waitForWatcherToStart(watcher);

        // Modify the watched file.
        FileUtils.writeStringToFile(tempFile, "hello", StandardCharsets.UTF_8, true);
        watcher.join(15 * 1000); // Wait for max 15 seconds for the thread to die.

        assertTrue(isCallbackInvoked.get());

        watcher.stopMonitoring();
        cleanupTempFile(tempFile);
    }

    @Test
    public void testStartAndStopInSequence() throws IOException, InterruptedException {
        FileModificationEventWatcher watcher = (FileModificationEventWatcher) this.prepareObjectUnderTest(
                SHARED_FILE.toPath());
        watcher.startMonitoring();

        waitForWatcherToStart(watcher);

        watcher.stopMonitoring();
    }

    @Test
    public void testSetsUpWatchDirAndFileNamesCorrectly() throws IOException {
        FileModificationEventWatcher watcher = new FileModificationEventWatcher(SHARED_FILE.toPath(),
                NOOP_CONSUMER);

        assertEquals(SHARED_FILE.toPath().getParent(), watcher.getWatchedDirectory());
        assertEquals(SHARED_FILE.getName(), watcher.getWatchedFileName());
    }

    @Test
    public void testWatcherThreadNameIsCustom() throws FileNotFoundException {
        FileModificationEventWatcher watcher = new FileModificationEventWatcher(
                PATH_VALID_NONEXISTENT, NOOP_CONSUMER, true, false);
        assertTrue(watcher.getName().startsWith("pravega-file-watcher-"));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetWatchedFileNameThrowsExceptionForNullFileName() throws FileNotFoundException {
        Path path = this.prepareRootDirPath();

        FileModificationEventWatcher watcher = new FileModificationEventWatcher(path, NOOP_CONSUMER,
                true, false);
        watcher.getWatchedFileName();
    }

    private void waitForWatcherToStart(FileModificationEventWatcher watcher) throws InterruptedException {
        // Ensure the watch is registered.
        for (int i = 0; i < 10; i++) {
            if (watcher.isWatchRegistered()) {
                break;
            } else {
                Thread.sleep(100);
            }
        }
        if (!watcher.isWatchRegistered()) {
            throw new RuntimeException("Failed to start the watcher");
        }
    }
}
