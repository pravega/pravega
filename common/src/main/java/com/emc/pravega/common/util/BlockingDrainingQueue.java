/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.common.util;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.ObjectClosedException;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents a thread-safe queue that dequeues all elements at once. Blocks the Dequeue if empty until new elements arrive.
 *
 * @param <T> The type of the items in the queue.
 */
@ThreadSafe
public class BlockingDrainingQueue<T> {
    //region Members

    @GuardedBy("contents")
    private final ArrayList<T> contents;
    @GuardedBy("contents")
    private CompletableFuture<List<T>> pendingRequest;
    @GuardedBy("contents")
    private boolean closed;

    ///endregion

    // region Constructor

    /**
     * Creates a new instance of the BlockingDrainingQueue class.
     */
    public BlockingDrainingQueue() {
        this.contents = new ArrayList<>();
    }

    //endregion

    //region Operations

    /**
     * Closes the queue and prevents any other access to it. Any blocked call to takeAllItems() will fail with InterruptedException.
     *
     * @return If the queue has any more items in it, these will be returned here. The items are guaranteed not to be
     * returned both here and via takeAllItems().
     */
    public List<T> close() {
        CompletableFuture<List<T>> pendingRequest = null;
        List<T> result = null;
        synchronized (this.contents) {
            if (!this.closed) {
                this.closed = true;
                pendingRequest = this.pendingRequest;
                this.pendingRequest = null;
                result = fetchContents();
            }
        }

        if (pendingRequest != null) {
            pendingRequest.cancel(true);
        }

        return result != null ? result : new ArrayList<>();
    }

    /**
     * Adds a new item to the queue.
     *
     * @param item The item to append.
     * @throws ObjectClosedException If the Queue is closed.
     */
    public void add(T item) {
        CompletableFuture<List<T>> pendingRequest;
        List<T> result = null;
        synchronized (this.contents) {
            Exceptions.checkNotClosed(this.closed, this);
            this.contents.add(item);
            pendingRequest = this.pendingRequest;
            this.pendingRequest = null;
            if (pendingRequest != null) {
                result = fetchContents();
            }
        }

        if (pendingRequest != null) {
            pendingRequest.complete(result);
        }
    }

    /**
     * Returns all items from the queue. If the queue is empty, it blocks the call until items are available.
     * At the end of this call, the queue will be empty.
     *
     * @return All the items currently in the queue.
     * @throws ObjectClosedException If the Queue is closed.
     * @throws IllegalStateException If another call to takeAllEntries is in progress.
     */
    public CompletableFuture<List<T>> takeAllEntries() {
        synchronized (this.contents) {
            Preconditions.checkState(this.pendingRequest == null, "Cannot have more than one concurrent pending takeAllEntries request.");
            if (this.contents.size() > 0) {
                return CompletableFuture.completedFuture(fetchContents());
            } else {
                this.pendingRequest = new CompletableFuture<>();
                return this.pendingRequest;
            }
        }
    }

    @GuardedBy("contents")
    private List<T> fetchContents() {
        List<T> result = new ArrayList<>(this.contents);
        this.contents.clear();
        return result;
    }

    // endregion
}
