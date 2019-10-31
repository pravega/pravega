/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import io.pravega.common.Exceptions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Organizes PlaceholderReadResultEntries by their starting offset and provides efficient methods for retrieving those
 * whose offsets are below certain values.
 */
@ThreadSafe
class FutureReadResultEntryCollection {
    //region Members

    @GuardedBy("reads")
    private final PriorityQueue<FutureReadResultEntry> reads;
    @GuardedBy("reads")
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

    /**
     * Closes this instance of the FutureReadResultEntryCollection class.
     *
     * @return A List containing all currently registered FutureReadResultEntries.
     */
    public List<FutureReadResultEntry> close() {
        List<FutureReadResultEntry> result;
        synchronized (this.reads) {
            if (this.closed) {
                result = Collections.emptyList();
            } else {
                result = new ArrayList<>(this.reads);
                this.reads.clear();
                this.closed = true;
            }
        }

        return result;
    }

    /**
     * Adds a new Result Entry.
     *
     * @param entry The entry to add.
     */
    public void add(FutureReadResultEntry entry) {
        synchronized (this.reads) {
            Exceptions.checkNotClosed(this.closed, this);
            this.reads.add(entry);
        }
    }

    /**
     * Finds the Result Entries that have a starting offset before the given offset, removes them from the collection,
     * and returns them.
     *
     * @param maxOffset The offset to query against.
     */
    Collection<FutureReadResultEntry> poll(long maxOffset) {
        List<FutureReadResultEntry> result = new ArrayList<>();
        synchronized (this.reads) {
            Exceptions.checkNotClosed(this.closed, this);

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
     */
    Collection<FutureReadResultEntry> pollAll() {
        return poll(Long.MAX_VALUE);
    }

    static int entryComparator(FutureReadResultEntry e1, FutureReadResultEntry e2) {
        if (e1.getStreamSegmentOffset() < e2.getStreamSegmentOffset()) {
            return -1;
        } else if (e1.getStreamSegmentOffset() > e2.getStreamSegmentOffset()) {
            return 1;
        }

        return 0;
    }

    //endregion
}
