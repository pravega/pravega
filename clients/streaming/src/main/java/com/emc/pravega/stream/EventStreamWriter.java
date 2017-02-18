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
package com.emc.pravega.stream;

import java.util.UUID;
import java.util.concurrent.Future;

/**
 * A writer can write events to a stream.
 * 
 * This class is safe to use across threads, but doing so will not increase performance.
 * 
 * @param <Type> The type of events that go in this stream
 */
public interface EventStreamWriter<Type> extends AutoCloseable {
    
    /**
     * Send an event to the stream. Events that are written should appear in the stream exactly once
     * in the order they were written. (If there are multiple writers they may be interleaved but
     * any event written after the future of another writeEvent call has completed, will come after the fir)
     * 
     * 
     * Note that the implementation provides retry logic to handle connection failures and service
     * host failures. Internal retries will not violate the exactly once semantic so it is better to
     * rely on them than to wrap this with custom retry logic.
     * 
     * @param routingKey A free form string that is used to route messages to readers. Two events
     *        written with the same routingKey are guaranteed to be read in order. Two events with
     *        different routing keys may be read in parallel.
     * @param event The event to be written to the stream
     * @return A future that will complete when the event has been durably stored on the configured
     *         number of replicas, and is available for readers to see. This future may complete
     *         exceptionally if this cannot happen, however these exceptions are not transient
     *         failures. Failures that occur as a result of connection drops or host death are
     *         handled internally with multiple retires and exponential backoff. So there is no need
     *         to attempt to retry in the event of an exception.
     */
    Future<Void> writeEvent(String routingKey, Type event);

    /**
     * Start a new transaction on this stream.
     * 
     * @param transactionTimeout The number of milliseconds after now, that if commit has not been called by, the
     *            transaction may be aborted. Note that this should not be set unnecessarily high, as having long running
     *            transactions may interfere with a streams to scale in response to a change in rate. For this reason
     *            streams may configure an upper limit to this value.
     * @return A transaction through which multiple events can be written atomically.
     */
    Transaction<Type> beginTxn(long transactionTimeout);
    
    /**
     * Returns a previously created transaction.
     * 
     * @param transactionId The result retained from calling {@link Transaction#getTxnId()}
     * @return transaction object with given UUID
     */
    Transaction<Type> getTxn(UUID transactionId);

    /**
     * Returns the configuration that this writer was create with.
     *
     * @return writer configuration
     */
    EventWriterConfig getConfig();

    /**
     * Block until all events that have been passed to writeEvent's corresponding futures have completed.
     */
    void flush();

    /**
     * Calls flush and then closes the writer. (No further methods may be called)
     */
    @Override
    void close();
}
