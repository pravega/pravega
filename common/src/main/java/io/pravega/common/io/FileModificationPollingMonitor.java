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
import java.nio.file.Paths;
import java.util.function.Consumer;

@Slf4j
public class FileModificationPollingMonitor implements FileModificationMonitor {

    private final static int DEFAULT_POLL_INTERVAL = 10 * 1000; // milliseconds

    /**
     * The path of file to watch.
     */
    private final Path pathOfFileToWatch;

    /**
     * The action to perform when the specified file changes.
     */
    private final Consumer<File> callback;

    private FileAlterationMonitor monitor;

    /**
     * Creates a new instance.
     *
     * @param fileToWatch the file to watch
     * @param callback    the callback to invoke when a modification to the {@code fileToWatch} is detected
     * @throws NullPointerException if either {@code fileToWatch} or {@code callback} is null
     * @throws InvalidPathException if {@code fileToWatch} is invalid
     * @throws FileNotFoundException when a file at specified path {@code fileToWatch} does not exist
     * @throws NullPointerException if either {@code fileToWatch}  or {@code callback} is null
     */
    public FileModificationPollingMonitor(String fileToWatch, Consumer<File> callback)
            throws FileNotFoundException {
        this.pathOfFileToWatch = Paths.get(fileToWatch);
        this.validateInput(this.pathOfFileToWatch, callback);
        this.callback = callback;
    }

    @Override
    public void startMonitoring() {
        Path dirPath = pathOfFileToWatch.getParent();

        FileAlterationObserver observer = new FileAlterationObserver(dirPath.toString(), new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.getName().equalsIgnoreCase(pathOfFileToWatch.getFileName().toString());
            }
        });
        log.debug("Directory being watched is {}", dirPath);

        monitor = new FileAlterationMonitor(DEFAULT_POLL_INTERVAL);

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
            log.info("Done setting up file change monitor for file [{}]", this.pathOfFileToWatch.toString());
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
                log.warn("Failed in closing the monitor", e);
                // ignore
            }
        }
    }
}
