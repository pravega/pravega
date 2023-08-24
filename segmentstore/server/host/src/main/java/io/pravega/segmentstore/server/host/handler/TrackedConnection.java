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
import io.pravega.shared.protocol.netty.WireCommand;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Tracks outstanding data for a single connection and pauses or resumes reading from it as appropriate.
 */
@Slf4j
@RequiredArgsConstructor
public class TrackedConnection implements AutoCloseable {
    /**
     * The {@link ServerConnection} to manage.
     */
    @NonNull
    private final ServerConnection connection;
    /**
     * Global {@link ConnectionTracker}.
     */
    @NonNull
    private final ConnectionTracker connectionTracker;
    private final AtomicLong outstandingBytes = new AtomicLong();

    /**
     * To be used only in tests. Creates a new tracker.
     *
     * @param connection Connection.
     */
    @VisibleForTesting
    public TrackedConnection(ServerConnection connection) {
        this(connection, new ConnectionTracker());
    }

    /**
     * Updates the total outstanding byte count for this connection and pauses ({@link ServerConnection#pauseReading})
     * or resumes ({@link ServerConnection#resumeReading} as needed.
     *
     * @param delta The number of bytes to adjust by. May be negative.
     */
    void adjustOutstandingBytes(int delta) {
        long currentOutstanding = this.outstandingBytes.updateAndGet(p -> Math.max(0, p + delta));
        this.connectionTracker.updateOutstandingBytes(this.connection, delta, currentOutstanding);
    }

    /**
     * See {@link ServerConnection#send(WireCommand)}.
     *
     * @param cmd The {@link WireCommand} to send.
     */
    void send(WireCommand cmd) {
        this.connection.send(cmd);
        log.debug("Sent response: {}", cmd);
    }

    @Override
    public void close() {
        this.connection.close();
    }

    @Override
    public String toString() {
        return String.format("%s [%s/%s]", this.connection, this.outstandingBytes, this.connectionTracker.getTotalOutstanding());
    }
}
