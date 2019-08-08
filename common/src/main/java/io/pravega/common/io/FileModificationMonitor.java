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

import lombok.NonNull;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.function.Consumer;

public interface FileModificationMonitor {
    void startMonitoring();

    void stopMonitoring();

    default void validateInput(@NonNull Path filePath, @NonNull Consumer callback)
            throws FileNotFoundException {
        if (!filePath.toFile().exists()) {
            throw new FileNotFoundException(String.format("File [%s] does not exist.", filePath));
        }
    }
}
