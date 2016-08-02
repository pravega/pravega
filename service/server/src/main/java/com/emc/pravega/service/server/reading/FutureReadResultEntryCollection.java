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

package com.emc.pravega.service.server.reading;

import com.emc.nautilus.common.Exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CancellationException;

/**
 * Organizes PlaceholderReadResultEntries by their starting offset and provides efficient methods for retrieving those
 * whose offsets are below certain values.
 */
class FutureReadResultEntryCollection implements AutoCloseable {
    //region Members

    private final PriorityQueue<FutureReadResultEntry> reads;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FutureReadResultEntryCollection class.
     */
    FutureReadResultEntryCollection() {
        this.reads = new PriorityQueue<>(FutureReadResultEntryCollection::entryComparator);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.closed = true;
        cancelAll();
    }

    //endregion

    //region Operations

    /**
     * Adds a new Result Entry.
     *
     * @param entry
     */
    public void add(FutureReadResultEntry entry) {
        Exceptions.checkNotClosed(this.closed, this);

        synchronized (this.reads) {
            this.reads.add(entry);
        }
    }

    /**
     * Finds the Result Entries that have a starting offset before the given offset, removes them from the collection,
     * and returns them.
     *
     * @param maxOffset The offset to query against.
     * @return
     */
    Collection<FutureReadResultEntry> poll(long maxOffset) {
        Exceptions.checkNotClosed(this.closed, this);

        List<FutureReadResultEntry> result = new ArrayList<>();
        synchronized (this.reads) {
            // 'reads' is sorted by Starting Offset, in ascending order. As long as it is not empty and the
            // first entry overlaps the given offset by at least one byte, extract and return it.
            while (this.reads.size() > 0 && this.reads.peek().getStreamSegmentOffset() <= maxOffset) {
                result.add(this.reads.poll());
            }
        }

        return result;
    }

    /**
     * Removes and returns all the Result Entries in the collection.
     *
     * @return
     */
    Collection<FutureReadResultEntry> pollAll() {
        return poll(Long.MAX_VALUE);
    }

    /**
     * Cancels all Reads in this collection..
     */
    void cancelAll() {
        List<FutureReadResultEntry> toCancel;
        synchronized (this.reads) {
            toCancel = new ArrayList<>(this.reads);
            this.reads.clear();
        }

        CancellationException ce = new CancellationException();
        toCancel.forEach(e -> e.fail(ce));
    }

    protected static int entryComparator(FutureReadResultEntry e1, FutureReadResultEntry e2) {
        if (e1.getStreamSegmentOffset() < e2.getStreamSegmentOffset()) {
            return -1;
        } else if (e1.getStreamSegmentOffset() > e2.getStreamSegmentOffset()) {
            return 1;
        }

        return 0;
    }

    //endregion
}
