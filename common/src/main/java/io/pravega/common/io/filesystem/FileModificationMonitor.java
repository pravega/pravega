/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.filesystem;

import lombok.NonNull;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Represents an object that monitors modifications to a file.
 *
 * By modification, we mean:
 * - Edits to an existing file
 * - Replacement of the whole file
 */
public interface FileModificationMonitor {

    /**
     * Start monitoring.
     */
    void startMonitoring();

    /**
     * Stop monitoring.
     */
    void stopMonitoring();

    /**
     * A convenience method for implementations.
     *
     * @param filePath the path of the file to be monitored
     * @param callback the callback to be invoked when a file modification is detected
     * @throws FileNotFoundException if the specified {@code filePath} does not exist
     * @throws NullPointerException if either {@code filePath} or {@code callback} is null
     */
    default void validateInput(@NonNull Path filePath, @NonNull Consumer callback)
            throws FileNotFoundException {
        if (!filePath.toFile().exists()) {
            throw new FileNotFoundException(String.format("File [%s] does not exist.", filePath));
        }
    }
}
