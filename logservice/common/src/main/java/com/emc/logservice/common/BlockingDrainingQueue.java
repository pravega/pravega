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

package com.emc.logservice.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a thread-safe queue that dequeues all elements at once. Blocks the Dequeue if empty until new elements arrive.
 *
 * @param <T> The type of the items in the queue.
 */
public class BlockingDrainingQueue<T> implements AutoCloseable {
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

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            lock.lock();
            try {
                this.notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    //endregion

    /**
     * Adds a new item to the queue.
     *
     * @param item The item to append.
     * @throws ObjectClosedException If the Queue is closed.
     */
    public void add(T item) {
        Exceptions.checkNotClosed(this.closed, this);

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            this.contents.add(item);
            this.notEmpty.signal();
        } finally {
            lock.unlock();
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
    public List<T> takeAllEntries() throws InterruptedException {
        Exceptions.checkNotClosed(this.closed, this);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            while (this.contents.isEmpty()) {
                this.notEmpty.await();
                if (this.closed) {
                    throw new InterruptedException();
                }
            }

            List<T> result = new ArrayList<>(this.contents);
            this.contents.clear();
            return result;
        } finally {
            lock.unlock();
        }
    }
}
