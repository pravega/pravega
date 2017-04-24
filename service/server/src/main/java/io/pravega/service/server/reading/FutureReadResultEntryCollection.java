/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.reading;

import io.pravega.common.Exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CancellationException;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Organizes PlaceholderReadResultEntries by their starting offset and provides efficient methods for retrieving those
 * whose offsets are below certain values.
 */
@ThreadSafe
class FutureReadResultEntryCollection implements AutoCloseable {
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

    //region AutoCloseable Implementation

    @Override
    public void close() {
        synchronized (this.reads) {
            if (this.closed) {
                return;
            }

            this.closed = true;
        }

        cancelAll();
    }

    //endregion

    //region Operations

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
