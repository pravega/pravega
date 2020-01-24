/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import java.util.List;

/**
 * Defines a read-only view of the BookKeeper Log Metadata.
 */
public interface ReadOnlyLogMetadata {
    /**
     * Gets a value indicating the current epoch of this LogMetadata. This changes upon every successful log initialization,
     * immediately after it was fenced.
     *
     * @return The current epoch.
     */
    long getEpoch();

    /**
     * Gets a value indicating the current version of the Metadata (this changes upon every successful metadata persist).
     * Note: this is different from getEpoch() - which gets incremented with every successful recovery.
     *
     * @return The current version.
     */
    int getUpdateVersion();

    /**
     * Gets a value indicating whether this log is enabled or not.
     *
     * @return True if enabled, false otherwise.
     */
    boolean isEnabled();

    /**
     * Gets a read-only ordered list of LedgerMetadata instances representing the Ledgers that currently make up this
     * Log Metadata.
     *
     * @return A new read-only list.
     */
    List<LedgerMetadata> getLedgers();

    /**
     * Gets a LedgerAddress representing the first location in the log that is accessible for reads.
     *
     * @return The Truncation Address.
     */
    LedgerAddress getTruncationAddress();
}
