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
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileModificationPollingMonitorTests extends FileModificationMonitorTests {

    /**
     * Holds a file created for shared use of tests. The lifecycle of this file is managed in this class. No tests
     * should write to this file or delete this file.
     */
    private final static File SHARED_FILE;

    private final static Consumer<File> NOOP_CONSUMER = c -> { };

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
        return new FileModificationPollingMonitor(path, NOOP_CONSUMER,
                FileModificationPollingMonitor.DEFAULT_POLL_INTERVAL, checkForFileExistence);
    }

    @Test(timeout = 2000)
    public void testInvokesCallBackForFileModification() throws IOException, InterruptedException {
        File tempFile = createTempFile();

        AtomicBoolean isCallbackInvoked = new AtomicBoolean(false);
        FileModificationMonitor monitor = new FileModificationPollingMonitor(tempFile.toPath(),
                c -> isCallbackInvoked.set(true), 100, true);
        monitor.startMonitoring();

        // Modify the watched file.
        FileUtils.writeStringToFile(tempFile, "hello", StandardCharsets.UTF_8, true);

        // Wait for some time
        Thread.sleep(500);

        assertTrue(isCallbackInvoked.get());
        monitor.stopMonitoring();

        cleanupTempFile(tempFile);
    }

    @Test (expected = IllegalStateException.class)
    public void testStartMonitoringChecksForState() throws FileNotFoundException {
        FileModificationMonitor monitor = new FileModificationPollingMonitor(Paths.get(File.pathSeparator),
                NOOP_CONSUMER, FileModificationPollingMonitor.DEFAULT_POLL_INTERVAL, false);
        monitor.startMonitoring();
    }

    @Test
    public void testPollingInterval() throws FileNotFoundException {
        FileModificationPollingMonitor monitor = new FileModificationPollingMonitor(
                FileModificationMonitorTests.PATH_VALID_NONEXISTENT, NOOP_CONSUMER,
                100, false);

        assertEquals(100, monitor.getPollingInterval());

        monitor = new FileModificationPollingMonitor(SHARED_FILE.toPath(), NOOP_CONSUMER);
        assertEquals(FileModificationPollingMonitor.DEFAULT_POLL_INTERVAL, monitor.getPollingInterval());
    }

    @Test (expected = IllegalStateException.class)
    public void testStartMonitoringThrowsExceptionWhenFileNameIsNull() throws FileNotFoundException {
        Path path = this.prepareRootDirPath();
        FileModificationPollingMonitor monitor = new FileModificationPollingMonitor(path, NOOP_CONSUMER, 100, false);
        monitor.startMonitoring();
    }
}
