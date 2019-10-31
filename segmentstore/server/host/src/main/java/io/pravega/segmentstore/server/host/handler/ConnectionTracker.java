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
import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks outstanding data for all connections and provides hints as to when to pause or resume reading from them.
 */
public class ConnectionTracker {
    /**
     * Threshold under which any connection may be resumed, subject to total connections not exceeding
     * {@link #DEFAULT_ALL_CONNECTIONS_MAX_OUTSTANDING_BYTES}.
     */
    @VisibleForTesting
    static final int LOW_WATERMARK = 1024 * 1024; //1MB
    /**
     * Maximum allowed outstanding bytes from all connections. If we exceed this value, all connections should be paused.
     */
    private static final int DEFAULT_ALL_CONNECTIONS_MAX_OUTSTANDING_BYTES = 512 * 1024 * 1024;
    /**
     * Maximum allowed outstanding bytes from a single connection. If we exceed this value, that connection should be paused.
     */
    private static final int DEFAULT_SINGLE_CONNECTION_MAX_OUTSTANDING = DEFAULT_ALL_CONNECTIONS_MAX_OUTSTANDING_BYTES / 4;

    private final int allConnectionsLimit;
    private final int singleConnectionDoubleLimit;
    private final AtomicLong totalOutstanding = new AtomicLong(0);

    public ConnectionTracker() {
        this(DEFAULT_ALL_CONNECTIONS_MAX_OUTSTANDING_BYTES, DEFAULT_SINGLE_CONNECTION_MAX_OUTSTANDING);
    }

    ConnectionTracker(int allConnectionsMaxOutstandingBytes, int singleConnectionMaxOutstandingBytes) {
        Preconditions.checkArgument(allConnectionsMaxOutstandingBytes >= LOW_WATERMARK,
                "allConnectionsMaxOutstandingBytes must be a value greater than %s.", LOW_WATERMARK);
        Preconditions.checkArgument(singleConnectionMaxOutstandingBytes >= LOW_WATERMARK,
                "singleConnectionMaxOutstandingBytes must be a value greater than %s.", LOW_WATERMARK);
        Preconditions.checkArgument(singleConnectionMaxOutstandingBytes <= allConnectionsMaxOutstandingBytes,
                "singleConnectionMaxOutstandingBytes (%s) must be at most allConnectionsMaxOutstandingBytes (%s).",
                singleConnectionMaxOutstandingBytes, allConnectionsMaxOutstandingBytes);

        this.allConnectionsLimit = allConnectionsMaxOutstandingBytes;
        this.singleConnectionDoubleLimit = 2 * singleConnectionMaxOutstandingBytes;
    }

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
    boolean adjustOutstandingBytes(long deltaBytes, long connectionOutstandingBytes) {
        // Perform quick sanity checks as assertions: these should pop up during tests but since this method is invoked
        // very frequently we do not want them enabled for production use.
        // Sanity Check #1: If a connection increased by an amount, its total outstanding should be at least that value.
        assert deltaBytes <= connectionOutstandingBytes : "connection delta greater than connection outstanding";
        long total = this.totalOutstanding.updateAndGet(p -> Math.max(0, p + deltaBytes));
        if (total >= this.allConnectionsLimit) {
            // Sum of all connections exceeded our capacity. Pause all of them.
            return false;
        }

        // Sanity check #2: No connection may have more outstanding than the total.
        assert connectionOutstandingBytes <= total : "single connection outstanding greater than total outstanding";
        return connectionOutstandingBytes < LOW_WATERMARK
                || connectionOutstandingBytes < (this.singleConnectionDoubleLimit - total);
    }
}
