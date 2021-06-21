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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Watches for modifications to the specified file and performs the specified action (in the form of a callback) upon
 * modification detection.
 *
 * This implementation uses NIO and event-based Java WatchService to watch for any modifications to the specified file.
 *
 * Note that:
 *
 * - The specified actions may trigger slightly later than when the file is actually modified.
 *
 * - If there are multiple file modifications in quick succession, only the last one may trigger the specified action.
 *
 * - Under rare circumstances, the callback is invoked twice upon file modification. The Java {@link WatchService}
 *   raises two events per modification: one for the content and second for the modification time. While there is a
 *   de-duplication logic in this class to ensure that only a single event is handled and notified to the callback,
 *   it may not work at times. If the calling code needs to be notified exactly once at all times, it should consider
 *   using {@link FileModificationPollingMonitor} instead.
 *
 * - This class won't work for a symbolic link file. This is because the Java {@link WatchService} does not raise
 *   events for modifications of such files. Consider using {@link FileModificationPollingMonitor} instead, when
 *   monitoring modifications to such files.
 *
 */
@Slf4j
public class FileModificationEventWatcher extends Thread implements FileModificationMonitor {

    private static final AtomicInteger THREAD_NUM = new AtomicInteger();

    /**
     * The path of file to watch.
     */
    private final Path watchedFilePath;

    /**
     * The action to perform when the specified file changes.
     */
    private final Consumer<WatchEvent<?>> callback;

    private final UncaughtExceptionHandler uncaughtExceptionalHandler = (t, e) -> logException(e);

    private final boolean loopContinuously;

    private boolean isWatchRegistered = false;

    /**
     * Creates a new instance.
     *
     * @param fileToWatch the file to watch
     * @param callback    the callback to invoke when a modification to the {@code fileToWatch} is detected
     * @throws NullPointerException if either {@code fileToWatch} or {@code callback} is null
     * @throws InvalidPathException if {@code fileToWatch} is invalid
     * @throws FileNotFoundException when a file at specified path {@code fileToWatch} does not exist
     */
    public FileModificationEventWatcher(@NonNull Path fileToWatch, @NonNull Consumer<WatchEvent<?>> callback)
            throws FileNotFoundException {
        this(fileToWatch, callback, true, true);
    }

    /**
     * Creates a new instance.
     *
     * @param fileToWatch path of the file to watch
     * @param callback          the callback to invoke when a modification to the {@code fileToWatch} is detected
     * @param loopContinuously  whether to keep continue to look for file modification after one iteration. This option
     *                          is useful for testing only.
     * @throws InvalidPathException if {@code fileToWatch} is invalid
     * @throws FileNotFoundException when a file at specified path {@code fileToWatch} does not exist
     * @throws NullPointerException if either {@code fileToWatch} or {@code callback} is null
     */
    @VisibleForTesting
    FileModificationEventWatcher(@NonNull Path fileToWatch, @NonNull Consumer<WatchEvent<?>> callback,
                                 boolean loopContinuously, boolean checkForFileExistence)
            throws FileNotFoundException {

        // Set the name for this object/thread for identification purposes.
        super("pravega-file-watcher-" + THREAD_NUM.incrementAndGet());
        Exceptions.checkNotNullOrEmpty(fileToWatch.toString(), "fileToWatch");
        if (checkForFileExistence && !fileToWatch.toFile().exists()) {
            throw new FileNotFoundException(String.format("File [%s] does not exist.", fileToWatch));
        }

        this.watchedFilePath = fileToWatch;
        this.callback = callback;
        this.loopContinuously = loopContinuously;
        setUncaughtExceptionHandler(uncaughtExceptionalHandler);
    }

    @VisibleForTesting
    String getWatchedFileName() {
        Path fileName = this.watchedFilePath.getFileName();
        if (fileName != null) {
            return fileName.toString();
        } else {
            // It should never happen that file name is null, but we have to perform this defensive null check to
            // satisfy SpotBugs. File name will be null for a path like "/".
            throw new IllegalStateException("File name is null");
        }
    }

    @VisibleForTesting
    Path getWatchedDirectory() {
        return this.watchedFilePath.getParent();
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run() {
        WatchKey watchKey = null;
        WatchService watchService = null;
        try {
            watchService = FileSystems.getDefault().newWatchService();
            log.debug("Done creating watch service for watching file at path: {}", this.watchedFilePath);

            String fileName = getWatchedFileName();
            Path directoryPath = getWatchedDirectory();
            log.debug("Directory being watched is {}", directoryPath);

            assert directoryPath != null;
            directoryPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_CREATE);
            log.debug("Registered the watch for the file: {}", this.watchedFilePath);

            isWatchRegistered = true;

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    watchKey = retrieveWatchKeyFrom(watchService);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logException(e);
                    // Allow thread to exit.
                }

                if (watchKey != null) {
                    Optional<WatchEvent<?>> modificationDetectionEvent = watchKey.pollEvents()
                            .stream()
                            .filter( // we only care about changes to the specified file.
                                    event -> event.context().toString().contains(fileName))
                            .findAny();

                    if (modificationDetectionEvent.isPresent()) {
                        log.info("Detected that the file [{}] has modified", this.watchedFilePath);
                        callback.accept(modificationDetectionEvent.get());
                    }

                    boolean isKeyValid = watchKey.reset();
                    log.debug("Done resetting watch key.");
                    if (!isKeyValid) {
                        log.info("No longer watching file [{}]", this.watchedFilePath);
                        break;
                    }
                }
                if (!loopContinuously) {
                    break;
                }
            }
        } catch (IOException e) {
            logException(e);
            throw new RuntimeException(e);
        } finally {
            if (watchKey != null) {
                watchKey.cancel();
            }
            if (watchService != null) {
                try {
                    watchService.close();
                } catch (IOException e) {
                    log.warn("Error closing watch service", e);
                }
            }
        }
        log.info("Thread [{}], watching for modifications in file [{}] exiting,", getName(), this.watchedFilePath);
    }

    private WatchKey retrieveWatchKeyFrom(WatchService watchService) throws InterruptedException {
        WatchKey result = watchService.take();
        log.info("Retrieved and removed watch key for watching file at path: {}", this.watchedFilePath);

        // Each file modification/create usually results in the WatcherService reporting the WatchEvent twice,
        // as the file is updated twice: once for the content and once for the file modification time.
        // These events occur in quick succession. We wait for 200 ms., here so that the events get
        // de-duplicated - in other words only single event is processed.
        //
        // See https://stackoverflow.com/questions/16777869/java-7-watchservice-ignoring-multiple-occurrences-
        // of-the-same-event for a discussion on this topic.
        //
        // If the two events are not raised within this duration, the callback will be invoked twice, which we
        // assume is not a problem for applications of this object. In case the applications do care about
        // being notified only once for each modification, they should use the FileModificationPollingMonitor
        // instead.
        Thread.sleep(200);

        return result;
    }

    @Override
    public void startMonitoring() {
        this.setDaemon(true);
        this.start();
        log.info("Completed setting up monitor for watching modifications to file: {}", this.watchedFilePath);
    }

    @VisibleForTesting
    boolean isWatchRegistered() {
        return isWatchRegistered;
    }

    @Override
    public void stopMonitoring() {
        this.interrupt();
        log.info("Stopped the monitor that was watching modifications to file {}", this.watchedFilePath);
    }

    private void logException(Throwable e) {
        log.warn("Thread [{}], watching for modifications in file [{}], encountered exception with cause [{}]",
                this.getName(), this.watchedFilePath, e.getMessage());
    }
}
