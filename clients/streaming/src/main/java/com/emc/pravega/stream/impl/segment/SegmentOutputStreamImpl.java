/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import static com.emc.pravega.common.Exceptions.handleInterrupted;
import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.concurrent.GuardedBy;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.WireCommands.Append;
import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.NoSuchBatch;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentIsSealed;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.common.util.Retry.RetryWithBackoff;
import com.emc.pravega.common.util.ReusableLatch;
import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.Unpooled;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

/**
 * Tracks inflight events, and manages reconnects automatically.
 * 
 * @see SegmentOutputStream
 */
@RequiredArgsConstructor
@Slf4j
class SegmentOutputStreamImpl extends SegmentOutputStream {

    private static final RetryWithBackoff RETRY_SCHEDULE = Retry.withExpBackoff(1, 10, 5);
    private final ConnectionFactory connectionFactory;
    private final String endpoint;
    private final UUID connectionId;
    private final String segment;
    private final State state = new State();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();

    /**
     * Internal object that tracks the state of the connection.
     * All mutations of data occur inside of this class. All operations are protected by the lock object.
     * No calls to external classes occur. No network calls occur via any methods in this object.
     */
    private static final class State {
        private final Object lock = new Object();
        @GuardedBy("lock")
        private boolean closed = false;
        @GuardedBy("lock")
        private ClientConnection connection;
        @GuardedBy("lock")
        private Exception exception = null;
        @GuardedBy("lock")
        private final ConcurrentSkipListMap<Append, CompletableFuture<Void>> inflight = new ConcurrentSkipListMap<>();
        @GuardedBy("lock")
        private long eventNumber = 0;
        private final ReusableLatch connectionSetup = new ReusableLatch();
        private final ReusableLatch inflightEmpty = new ReusableLatch(true);

        /**
         * Blocks until there are no more messages inflight. (No locking required)
         */
        private void waitForEmptyInflight() {
            handleInterrupted(() -> inflightEmpty.await());
        }

        private void connectionSetupComplete(long ackLevel) {
            synchronized (lock) {
                eventNumber = ackLevel;
                connectionSetup.release();
            }
        }

        /**
         * @return The current connection (May be null if not connected)
         */
        private ClientConnection getConnection() {
            synchronized (lock) {
                return connection;
            }
        }

        /**
         * @param newConnection The new connection that has been established that should used going forward.
         */
        private void newConnection(ClientConnection newConnection) {
            synchronized (lock) {
                connectionSetup.reset();
                exception = null;
                connection = newConnection;
            }
        }

        /**
         * @param e Error that has occurred that needs to be handled by tearing down the connection.
         */
        private void failConnection(Exception e) {
            log.warn("Connection failed due to", e);
            ClientConnection oldConnection;
            synchronized (lock) {
                if (exception == null) {
                    exception = e;
                }
                oldConnection = connection;
                connection = null;
            }
            connectionSetupComplete(0);
            oldConnection.close();
        }

        /**
         * Block until a connection has been established and AppendSetup has come back from the server.
         */
        private ClientConnection waitForConnection() throws ConnectionFailedException, SegmentSealedException {
            try {
                Exceptions.handleInterrupted(() -> connectionSetup.await());
                synchronized (lock) {
                    if (exception != null) {
                        throw exception;
                    }
                    return connection;
                }
            } catch (ConnectionFailedException | IllegalArgumentException | SegmentSealedException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Add event to the infight
         */
        private Append createNewInflightAppend(UUID connectionId, String segment, ByteBuffer buff,
                CompletableFuture<Void> callback) {
            synchronized (lock) {
                eventNumber++;
                Append append = new Append(segment, connectionId, eventNumber, Unpooled.wrappedBuffer(buff));
                inflightEmpty.reset();
                inflight.put(append, callback);
                return append;
            }
        }

        /**
         * Remove all events with event numbers below the provided level from inflight and return them.
         */
        private List<CompletableFuture<Void>> removeInflightBelow(long ackLevel) {
            synchronized (lock) {
                ArrayList<CompletableFuture<Void>> result = new ArrayList<>();
                for (Iterator<Entry<Append, CompletableFuture<Void>>> iter = inflight.entrySet().iterator(); iter.hasNext();) {
                    Entry<Append, CompletableFuture<Void>> append = iter.next();
                    if (append.getKey().getEventNumber() <= ackLevel) {
                        result.add(append.getValue());
                        iter.remove();
                    } else {
                        break;
                    }
                }
                if (inflight.isEmpty()) {
                    inflightEmpty.release();
                }
                return result;
            }
        }

        private List<Append> getAllInflight() {
            synchronized (lock) {
                return new ArrayList<>(inflight.keySet());
            }
        }

        private boolean isClosed() {
            synchronized (lock) {
                return closed;
            }
        }

        private void setClosed(boolean closed) {
            synchronized (lock) {
                this.closed = closed;
            }
        }
    }

    private final class ResponseProcessor extends FailingReplyProcessor {

        @Override
        public void wrongHost(WrongHost wrongHost) {
            state.failConnection(new ConnectionFailedException()); // TODO: Probably something else.
        }

        @Override
        public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
            state.failConnection(new SegmentSealedException());
        }

        @Override
        public void noSuchSegment(NoSuchSegment noSuchSegment) {
            state.failConnection(new IllegalArgumentException(noSuchSegment.toString()));
        }

        @Override
        public void noSuchBatch(NoSuchBatch noSuchBatch) {
            state.failConnection(new IllegalArgumentException(noSuchBatch.toString()));
        }

        @Override
        public void dataAppended(DataAppended dataAppended) {
            long ackLevel = dataAppended.getEventNumber();
            ackUpTo(ackLevel);
        }

        @Override
        public void appendSetup(AppendSetup appendSetup) {
            long ackLevel = appendSetup.getLastEventNumber();
            ackUpTo(ackLevel);
            try {
                retransmitInflight();
                state.connectionSetupComplete(ackLevel);
            } catch (ConnectionFailedException e) {
                state.failConnection(e);
            }
        }

        private void ackUpTo(long ackLevel) {
            for (CompletableFuture<Void> toAck : state.removeInflightBelow(ackLevel)) {
                if (toAck != null) {
                    toAck.complete(null);
                }
            }
        }

        private void retransmitInflight() throws ConnectionFailedException {
            for (Append append : state.getAllInflight()) {
                state.connection.send(append);
            }
        }
    }
    
    /**
     * @see com.emc.pravega.stream.impl.segment.SegmentOutputStream#write(java.nio.ByteBuffer,
     *      java.util.concurrent.CompletableFuture)
     */
    @Override
    @Synchronized
    public void write(ByteBuffer buff, CompletableFuture<Void> callback) throws SegmentSealedException {
        checkArgument(buff.remaining() <= SegmentOutputStream.MAX_WRITE_SIZE, "Write size too large: %s", buff.remaining());
        ClientConnection connection = getConnection();
        Append append = state.createNewInflightAppend(connectionId, segment, buff, callback);
        try {
            connection.send(append);
        } catch (ConnectionFailedException e) {
            log.warn("Connection failed due to: ", e);
            getConnection(); // As the messages is inflight, this will perform the retransmition.
        }
    }

    /**
     * Blocking call to establish a connection and wait for it to be setup. (Retries built in)
     */
    @Synchronized
    ClientConnection getConnection() throws SegmentSealedException {
        checkState(!state.isClosed(), "LogOutputStream was already closed");
        return RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class).throwingOn(SegmentSealedException.class).run(() -> {
            setupConnection();
            return state.waitForConnection();
        });
    }

    @Synchronized
    @VisibleForTesting
    void setupConnection() throws ConnectionFailedException {
        if (state.getConnection() == null) {
            ClientConnection connection = getAndHandleExceptions(connectionFactory
                .establishConnection(endpoint, responseProcessor), ConnectionFailedException::new);
            state.newConnection(connection);
            SetupAppend cmd = new SetupAppend(connectionId, segment);
            connection.send(cmd);
        }
    }

    /**
     * @see com.emc.pravega.stream.impl.segment.SegmentOutputStream#close()
     */
    @Override
    @Synchronized
    public void close() throws SegmentSealedException {
        if (state.isClosed()) {
            return;
        }
        flush();
        state.setClosed(true);
        ClientConnection connection = state.getConnection();
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * @see com.emc.pravega.stream.impl.segment.SegmentOutputStream#flush()
     */
    @Override
    @Synchronized
    public void flush() throws SegmentSealedException {
        try {
            ClientConnection connection = getConnection();
            connection.send(new KeepAlive());
            state.waitForEmptyInflight();
        } catch (ConnectionFailedException e) {
            state.failConnection(e);
        }
    }

}
