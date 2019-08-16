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
}
