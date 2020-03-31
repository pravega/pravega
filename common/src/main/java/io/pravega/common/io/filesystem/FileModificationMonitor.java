/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
<<<<<<< HEAD:common/src/main/java/io/pravega/common/io/filesystem/FileModificationMonitor.java
package io.pravega.common.io.filesystem;

/**
 * Represents an object that monitors modifications to a file.
=======
package io.pravega.client.tables;

import lombok.Data;
import lombok.NonNull;

/**
 * A {@link KeyValueTable} Key with a {@link Version}.
>>>>>>> Issue 4568: Key-Value Table Client Contracts (#4588):client/src/main/java/io/pravega/client/tables/TableKey.java
 *
 * By modification, we mean:
 * - Edits to an existing file
 * - Replacement of the whole file
 */
<<<<<<< HEAD:common/src/main/java/io/pravega/common/io/filesystem/FileModificationMonitor.java
public interface FileModificationMonitor {
    /**
     * Start monitoring.
     */
    void startMonitoring();

    /**
     * Stop monitoring.
     */
    void stopMonitoring();
=======
@Data
public class TableKey<KeyT> {
    /**
     * The Key.
     */
    @NonNull
    private final KeyT key;

    /**
     * The Version. If null, any updates for this Key will be unconditional. See {@link KeyValueTable} for details on
     * conditional updates.
     */
    private final Version version;
>>>>>>> Issue 4568: Key-Value Table Client Contracts (#4588):client/src/main/java/io/pravega/client/tables/TableKey.java
}
