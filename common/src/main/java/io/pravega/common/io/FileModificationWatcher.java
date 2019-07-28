/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Monitors modifications to the specified file and perform a specified action (in the form of a callback) upon
 * modification detection.
 *
 * Note that:
 * - The specified actions may trigger slightly later than when the file is actually modified.
 * - If there are multiple file modifications in quick succession, only the last one may trigger the specified action.
 *
 */
@Slf4j
public class FileModificationWatcher extends Thread {

    private static final AtomicInteger THREAD_NUM = new AtomicInteger();

    /**
     * The path of file to watch.
     */
    private final Path pathOfFileToWatch;

    /**
     * The action to perform when the specified file changes.
     */
    private final Consumer<WatchEvent<?>> callback;

    private final UncaughtExceptionHandler uncaughtExceptionalHandler = (t, e) -> logException(e);

    private boolean loopContinuously;

    /**
     * Creates a new instance.
     *
     * @param fileToWatch the file to watch
     * @param callback    the callback to invoke when a modification to the {@code fileToWatch} is detected
     * @throws NullPointerException if either {@code fileToWatch} or {@code callback} is null
     * @throws InvalidPathException if {@code fileToWatch} is invalid
     * @throws FileNotFoundException when a file at specified path {@code fileToWatch} does not exist
     */
    public FileModificationWatcher(@NonNull String fileToWatch, @NonNull Consumer<WatchEvent<?>> callback)
            throws FileNotFoundException {
        this(Paths.get(fileToWatch), callback, true);
    }

    /**
     * Creates a new instance.
     *
     * @param pathOfFileToWatch path of the file to watch
     * @param callback          the callback to invoke when a modification to the {@code fileToWatch} is detected
     * @param loopContinuously  whether to keep the thread look for file modification after one iteration. This option
     *                          is useful for testing only.
     * @throws InvalidPathException if {@code fileToWatch} is invalid
     * @throws FileNotFoundException when a file at specified path {@code fileToWatch} does not exist
     * @throws FileNotFoundException when a file at specified path {@code fileToWatch} does not exist
     */
    @VisibleForTesting
    FileModificationWatcher(@NonNull Path pathOfFileToWatch, @NonNull Consumer<WatchEvent<?>> callback,
                                   boolean loopContinuously)
            throws FileNotFoundException {
        super();

        this.pathOfFileToWatch = pathOfFileToWatch;

        if (!this.pathOfFileToWatch.toFile().exists()) {
            throw new FileNotFoundException(String.format("File [%s] does not exist.", pathOfFileToWatch));
        }
        this.callback = callback;
        setUncaughtExceptionHandler(uncaughtExceptionalHandler);

        // Set the name for this object/thread for identification purposes.
        this.setName("file-update-watcher-" + THREAD_NUM.incrementAndGet());
        this.loopContinuously = loopContinuously;
    }

    @VisibleForTesting
    String nameOfFileToWatch() {
        Path fileName = this.pathOfFileToWatch.getFileName();
        if (fileName != null) {
            return fileName.toString();
        } else {
            throw new RuntimeException("File name is null");
        }
    }

    @VisibleForTesting
    Path directoryOfFileToWatch() {
        assert this.pathOfFileToWatch != null;
        return this.pathOfFileToWatch.getParent();
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run() {
        WatchKey watchKey = null;
        WatchService watchService = null;
        try {
            watchService = FileSystems.getDefault().newWatchService();
            log.debug("Done creating watch service for watching file at path: {}", this.pathOfFileToWatch);

            String fileName = nameOfFileToWatch();
            Path directoryPath = directoryOfFileToWatch();
            log.debug("Directory being watched is {}", directoryPath);

            assert directoryPath != null;
            directoryPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_CREATE);
            log.debug("Done setting up watch for modify entries for file at path: {}", this.pathOfFileToWatch);

            while (true) {
                watchKey = watchService.take();
                log.info("Retrieved and removed watch key for watching file at path: {}", this.pathOfFileToWatch);

                // Looks odd, right? Using the logic or de-duplicating file change events, as suggested by some here:
                // https://stackoverflow.com/questions/16777869/java-7-watchservice-ignoring-multiple-occurrences-of-
                // the-same-event
                Thread.sleep(200);

                if (watchKey != null) {
                    Optional<WatchEvent<?>> modificationDetectionEvent = watchKey.pollEvents()
                            .stream()
                            .filter( // we only care about changes to the specified file.
                                    event -> event.context().toString().contains(fileName))
                            .findAny(); // we only care to know whether the

                    if (modificationDetectionEvent.isPresent()) {
                        log.debug("Detected that the file [{}] has modified", this.pathOfFileToWatch);
                        callback.accept(modificationDetectionEvent.get());
                    }

                    boolean isKeyValid = watchKey.reset();
                    log.debug("Done resetting watch key, so that it can receive further event notifications.");
                    if (!isKeyValid) {
                        log.info("No longer watching file [{}]", this.pathOfFileToWatch);
                        break;
                    }
                } else {
                    log.debug("watchKey for file at path {} was null", this.pathOfFileToWatch);
                }
                if (!loopContinuously) {
                    break;
                }
            }
        } catch (InterruptedException | IOException e) {
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
            log.info("Thread [{}], watching for modifications in file [{}] exiting,", getName(), this.pathOfFileToWatch);
        }
    }

    private void logException(Throwable e) {
        log.warn("Thread [{}], watching for modifications in file [{}], encountered exception with cause [{}]",
                this.getName(), this.pathOfFileToWatch, e.getMessage());
    }
}
