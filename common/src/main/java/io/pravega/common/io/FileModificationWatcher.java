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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.function.Consumer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * This class can be used to create objects that monitor modifications to the specified file and perform a
 * specified action (in the form of a callback) upon modification detection.
 *
 * Note that:
 * - The specified actions may trigger slightly later than when the file is actually modified.
 * - If there are multiple file modifications in quick succession, only the last one may trigger the specified action.
 *
 */
@Slf4j
public class FileModificationWatcher extends Thread {

    /**
     * The path of file to watch.
     */
    private final Path pathOfFileToWatch;

    /**
     * The action to perform when the specified file changes.
     */
    private final Consumer<WatchEvent<?>> callback;

    private UncaughtExceptionHandler uncaughtExceptionalHandler = (t, e) -> handleException(t.getName(), e);

    public FileModificationWatcher(@NonNull String fileToWatch, @NonNull Consumer<WatchEvent<?>> callback) {
        super();

        this.pathOfFileToWatch = Paths.get(fileToWatch);

        if (!this.pathOfFileToWatch.toFile().exists()) {
            throw new IllegalArgumentException(String.format("File [%s] does not exist.", fileToWatch));
        }
        this.callback = callback;
        setUncaughtExceptionHandler(uncaughtExceptionalHandler);
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run() {

        WatchKey watchKey = null;
        WatchService watchService = null;
        try {
            watchService = FileSystems.getDefault().newWatchService();
            log.debug("Done creating watch service.");

            String fileName = this.pathOfFileToWatch.getFileName().toString();
            Path directoryPath = this.pathOfFileToWatch.getParent();

            log.debug("Directory being watched is {}.", directoryPath);

            directoryPath.register(watchService,
                    StandardWatchEventKinds.ENTRY_MODIFY);
            log.debug("Done setting up watch for modify entries.");

            while (true) {
                watchKey = watchService.take();
                log.debug("Done setting up watch key.");

                // Looks odd, right? Using the logic or de-duplicating file change events, as suggested by some here:
                // https://stackoverflow.com/questions/16777869/java-7-watchservice-ignoring-multiple-occurrences-of-
                // the-same-event
                Thread.sleep(200);

                if (watchKey != null) {
                    log.debug("Watch key is not null");

                    watchKey.pollEvents()
                            .stream()
                            .filter(event -> // we only care about changes to the specified file.
                                    event.context().toString().contains(fileName))
                            .forEach(event ->  callback.accept(event)); // invoke the specified callback
                }

                boolean isKeyValid = watchKey.reset();
                log.debug("Done resetting watch key, so that it can receive further event notifications.");
                if (!isKeyValid) {
                    log.info("No longer watching file [{}]", this.pathOfFileToWatch);
                }
            }
        } catch (IOException | InterruptedException e) {
            log.warn(e.getMessage(), e);
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
            log.info("[{}] thread exiting.", getName());
        }
    }

    protected void handleException(String threadName, Throwable e) {
        log.warn("Exception occurred from thread {}", threadName, e);
    }
}