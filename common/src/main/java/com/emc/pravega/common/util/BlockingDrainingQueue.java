/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.ObjectClosedException;

/**
 * Represents a thread-safe queue that dequeues all elements at once. Blocks the Dequeue if empty until new elements arrive.
 *
 * @param <T> The type of the items in the queue.
 */
public class BlockingDrainingQueue<T> {
    //region Members

    private final ReentrantLock lock;
    private final Condition notEmpty;
    private final ArrayList<T> contents;
    private boolean closed;

    ///endregion

    // region Constructor

    /**
     * Creates a new instance of the BlockingDrainingQueue class.
     */
    public BlockingDrainingQueue() {
        this.lock = new ReentrantLock();
        this.notEmpty = lock.newCondition();
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
        lock.lock();
        try {
            if (!this.closed) {
                this.closed = true;
                this.notEmpty.signal();
                return swapContents();
            }
        } finally {
            lock.unlock();
        }

        return new ArrayList<>();
    }

    /**
     * Adds a new item to the queue.
     *
     * @param item The item to append.
     * @throws ObjectClosedException If the Queue is closed.
     */
    public void add(T item) {
        this.lock.lock();
        try {
            Exceptions.checkNotClosed(this.closed, this);
            this.contents.add(item);
            this.notEmpty.signal();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Returns all items from the queue. If the queue is empty, it blocks the call until items are available.
     * At the end of this call, the queue will be empty.
     *
     * @return All the items currently in the queue.
     * @throws ObjectClosedException If the Queue is closed.
     * @throws IllegalStateException If another call to takeAllEntries is in progress.
     * @throws InterruptedException  If the call is waiting for an empty queue to become non-empty and the queue is closed
     *                               while waiting.
     */
    public List<T> takeAllEntries() throws InterruptedException {
        this.lock.lock();
        try {
            Exceptions.checkNotClosed(this.closed, this);
            while (this.contents.isEmpty()) {
                this.notEmpty.await();
                if (this.closed) {
                    throw new InterruptedException();
                }
            }

            return swapContents();
        } finally {
            this.lock.unlock();
        }
    }

    private List<T> swapContents() {
        List<T> result = new ArrayList<>(this.contents);
        this.contents.clear();
        return result;
    }

    // endregion
}
