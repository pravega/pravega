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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Monitors for modifications to the specified file using a polling-based mechanism and performs the
 * specified action (in the form of a callback) upon detection of a file modification.
 *
 * Unlike {@link FileModificationEventWatcher}, which doesn't work for monitoring modifications to a file which is
 * a symbolic link to another file, an object of this class works for such files as well. However, since this object
 * uses a polling-based mechanism - unlike the event-based mechanism used by {@link FileModificationEventWatcher}, lists
 * files on each iteration and compares the output, this object is highly likely to be less efficient than an object of
 * {@link FileModificationEventWatcher} - CPU cycles-wise.
 *
 */
@Slf4j
public class FileModificationPollingMonitor implements FileModificationMonitor {

    @VisibleForTesting
    final static int DEFAULT_POLL_INTERVAL = 10 * 1000; // 10 seconds

    /**
     * The path of file to watch.
     */
    private final Path pathOfFileToWatch;

    /**
     * The action to perform when the a modification is detected for the specified file {@code pathOfFileToWatch}.
     */
    private final Consumer<File> callback;

    private final FileAlterationMonitor monitor;

    @VisibleForTesting
    @Getter
    private final int pollingInterval;

    /**
     * Creates a new instance.
     *
     * @param fileToWatch the file to watch
     * @param callback    the callback to invoke when a modification to the {@code fileToWatch} is detected
     *
     * @throws InvalidPathException if {@code fileToWatch} is invalid
     * @throws FileNotFoundException when a file at specified path {@code fileToWatch} does not exist
     * @throws NullPointerException if either {@code fileToWatch}  or {@code callback} is null
     */
    public FileModificationPollingMonitor(@NonNull Path fileToWatch, @NonNull Consumer<File> callback)
            throws FileNotFoundException {
        this(fileToWatch, callback, DEFAULT_POLL_INTERVAL, true);
    }

    @VisibleForTesting
    FileModificationPollingMonitor(@NonNull Path fileToWatch, @NonNull Consumer<File> callback, int pollingInterval,
                                   boolean checkForFileExistence) throws FileNotFoundException {

        Exceptions.checkNotNullOrEmpty(fileToWatch.toString(), "fileToWatch");
        if (checkForFileExistence && !fileToWatch.toFile().exists()) {
            throw new FileNotFoundException(String.format("File [%s] does not exist.", fileToWatch));
        }

        this.pathOfFileToWatch = fileToWatch;
        this.callback = callback;
        this.pollingInterval = pollingInterval;
        monitor = new FileAlterationMonitor(this.pollingInterval);
    }

    @Override
    public void startMonitoring() {
        Path fileName = pathOfFileToWatch.getFileName();
        log.debug("File name obtained from pathOfFileToWatch is [{}]", fileName);
        if (fileName == null) {
            throw new IllegalStateException("fileName is 'null'");
        }

        Path dirPath = pathOfFileToWatch.getParent();
        log.debug("dirPath is [{}]", dirPath);
        if (dirPath == null) {
            throw new IllegalStateException("The directory containing the file turned out to be 'null`");
        }

        FileAlterationObserver observer = new FileAlterationObserver(dirPath.toString(), new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (file == null || file.getName() == null) {
                    return false;
                }
                return file.getName().equals(fileName.toString());
            }
        });

        FileAlterationListener listener = new FileAlterationListenerAdaptor() {
            @Override
            public void onFileChange(File file) {
                log.info("Detected that the file [{}] has modified", file.getPath());
                callback.accept(file);
            }
        };

        observer.addListener(listener);
        monitor.addObserver(observer);
        try {
            monitor.start();
            log.info("Done setting up file modification monitor for file [{}]", this.pathOfFileToWatch);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopMonitoring() {
        if (monitor != null) {
            try {
                monitor.stop(5 * 1000);
            } catch (Exception e) {
                log.warn("Failed to close the monitor", e);
                // ignore
            }
        }
    }
}
