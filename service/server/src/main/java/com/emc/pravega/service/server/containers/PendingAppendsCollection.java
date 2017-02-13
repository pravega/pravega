/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps track of pending Appends (and their Append Contexts). This class holds information about such appends
 * only while they are in the Durable Log Operation Queue or in the Durable Log Queue Processor. As soon as the
 * related operation completes, all traces of its existence are removed from this collection.
 */
class PendingAppendsCollection implements AutoCloseable {
    //region Members

    private final ConcurrentHashMap<String, Entry> entries;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the PendingAppendsCollection class.
     */
    PendingAppendsCollection() {
        this.entries = new ConcurrentHashMap<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            List<Entry> allEntries = new ArrayList<>(this.entries.values());
            ObjectClosedException failException = new ObjectClosedException(this);
            allEntries.forEach(e -> e.completionFuture.completeExceptionally(failException));
            assert this.entries.size() == 0 : "Not all pending appends were cleaned up.";
        }
    }

    //endregion

    //region Operations

    /**
     * Registers (if necessary) the given StreamSegmentAppendOperation and attaches to its completion callback.
     *
     * @param operation          The StreamSegmentAppendOperation to register to.
     * @param completionCallback A CompletableFuture that will indicate the outcome of the operation.
     */
    void register(StreamSegmentAppendOperation operation, CompletableFuture<Long> completionCallback) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkNotNull(operation, "operation");
        Preconditions.checkNotNull(operation, "completionCallback");

        if (operation.getAppendContext() != null) {
            // Create an entry and put it in the map.
            Entry e = new Entry(getKey(operation.getStreamSegmentId(), operation.getAppendContext().getClientId()), operation.getAppendContext(), completionCallback);
            this.entries.put(e.key, e);

            // Upon completion, regardless of outcome, remove it from the index.
            e.completionFuture.whenComplete((r, ex) -> this.entries.remove(e.key));
        }
    }

    /**
     * Gets the last pending Append Context for the given StreamSegment and Client.
     *
     * @param streamSegmentId The Id of the StreamSegment to inquire for.
     * @param clientId        The Id of the Client to inquire for.
     * @return A CompletableFuture that, when completed, will contain the requested AppendContext. If the append failed,
     * the Future will contain the cause of the failure. If no pending appends exist for the given combination of arguments,
     * this method returns null.
     */
    public CompletableFuture<AppendContext> get(long streamSegmentId, UUID clientId) {
        Exceptions.checkNotClosed(this.closed, this);

        String key = getKey(streamSegmentId, clientId);
        Entry e = this.entries.getOrDefault(key, null);
        if (e == null) {
            return null;
        } else {
            return e.completionFuture;
        }
    }

    private String getKey(long streamSegmentId, UUID connectionId) {
        return Long.toHexString(streamSegmentId) + "|" + connectionId.toString();
    }

    //endregion

    //region Entry

    private static class Entry {
        public final String key;
        public final CompletableFuture<AppendContext> completionFuture;
        private final AppendContext context;

        public Entry(String key, AppendContext context, CompletableFuture<Long> operationCompletion) {
            this.key = key;
            this.context = context;
            this.completionFuture = operationCompletion.thenApply(o -> this.context);
        }

        @Override
        public String toString() {
            return String.format("%s: %s (%s)", this.key, this.context, this.completionFuture.isDone() ? (this.completionFuture.isCompletedExceptionally() ? "Error" : "Complete") : "Pending");
        }
    }

    //endregion
}
