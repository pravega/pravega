/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.host.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.store.ServiceConfig;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks outstanding data for all connections and pauses or resumes reading from them as appropriate.
 */
public class ConnectionTracker {
    /**
     * Threshold under which any connection may be resumed, subject to total connections not exceeding
     */
    @VisibleForTesting
    static final int LOW_WATERMARK = 1024 * 1024; //1MB

    private final int allConnectionsLimit;
    private final int singleConnectionDoubleLimit;
    private final AtomicLong totalOutstanding = new AtomicLong(0);

    public ConnectionTracker() {
        this(ServiceConfig.DEFAULT_ALL_CONNECTIONS_MAX_OUTSTANDING_BYTES.getDefaultValue().intValue(),
                ServiceConfig.DEFAULT_SINGLE_CONNECTION_MAX_OUTSTANDING_BYTES.getDefaultValue().intValue());
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
     * Updates the total outstanding byte count for the given connection and pauses ({@link ServerConnection#pauseReading})
     * or resumes ({@link ServerConnection#resumeReading} as needed.
     *
     * @param connection                 The {@link ServerConnection} to pause or resume if needed.
     * @param deltaBytes                 The number of bytes to adjust by. May be negative.
     * @param connectionOutstandingBytes The current number of outstanding bytes for the connection invoking this method.
     */
    void updateOutstandingBytes(ServerConnection connection, long deltaBytes, long connectionOutstandingBytes) {
        if (shouldContinueReading(deltaBytes, connectionOutstandingBytes)) {
            connection.resumeReading();
        } else {
            connection.pauseReading();
        }
    }

    /**
     * Updates the total outstanding byte count by the given value.
     *
     * @param deltaBytes                 The number of bytes to adjust by. May be negative.
     * @param connectionOutstandingBytes The current number of outstanding bytes for the connection invoking this method.
     * @return True if the connection should continue reading, false if it should pause.
     */
    private boolean shouldContinueReading(long deltaBytes, long connectionOutstandingBytes) {
        // Perform a sanity check as an assertion: it should pop up during tests but since this method is invoked
        // very frequently we do not want it enabled for production use.
        // If a connection increased by an amount, its total outstanding should be at least that value.
        assert deltaBytes <= connectionOutstandingBytes : "connection delta greater than connection outstanding";
        long total = this.totalOutstanding.updateAndGet(p -> Math.max(0, p + deltaBytes));
        if (total >= this.allConnectionsLimit) {
            // Sum of all connections exceeded our capacity. Pause all of them.
            return false;
        }

        return connectionOutstandingBytes < LOW_WATERMARK
                || connectionOutstandingBytes < (this.singleConnectionDoubleLimit - total);
    }
}
