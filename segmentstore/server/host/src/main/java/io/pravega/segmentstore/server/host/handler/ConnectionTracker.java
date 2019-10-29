/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks outstanding data for all connections and provides hints as to when to pause or resume reading from them.
 */
public class ConnectionTracker {
    /**
     * Maximum allowed outstanding bytes from all connections. If we exceed this value, all connections should be paused.
     */
    private static final int ALL_CONNECTIONS_MAX_OUTSTANDING_BYTES = 1024 * 1024 * 1024; // 1GB;
    /**
     * Maximum allowed outstanding bytes from a single connection. If we exceed this value, that connection should be paused.
     */
    private static final int SINGLE_CONNECTION_MAX_OUTSTANDING = ALL_CONNECTIONS_MAX_OUTSTANDING_BYTES / 2; // 512MB
    /**
     * Threshold under which any connection may be resumed, subject to total connections not exceeding
     * {@link #ALL_CONNECTIONS_MAX_OUTSTANDING_BYTES}.
     */
    private static final int LOW_WATERMARK = 1024 * 1024; //1MB
    private final AtomicLong totalOutstanding = new AtomicLong(0);

    /**
     * Gets a value indicating the total number of outstanding bytes across all connections.
     *
     * @return The value.
     */
    @VisibleForTesting
    long getTotalOutstanding() {
        return this.totalOutstanding.get();
    }

    /**
     * Updates the total outstanding byte count by the given value.
     *
     * @param deltaBytes                 The number of bytes to adjust by. May be negative.
     * @param connectionOutstandingBytes The current number of outstanding bytes for the connection invoking this method.
     * @return True if the connection should continue reading, false if it should pause.
     */
    public boolean adjustOutstandingBytes(long deltaBytes, long connectionOutstandingBytes) {
        long total = this.totalOutstanding.updateAndGet(p -> Math.max(0, p + deltaBytes));
        if (total >= ALL_CONNECTIONS_MAX_OUTSTANDING_BYTES) {
            // Sum of all connections exceeded our capacity. Pause all of them.
            return false;
        }

        return connectionOutstandingBytes < LOW_WATERMARK
                || connectionOutstandingBytes < (SINGLE_CONNECTION_MAX_OUTSTANDING - total);
    }
}
