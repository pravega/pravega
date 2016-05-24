package com.emc.logservice.reading;

import com.emc.logservice.core.ObjectClosedException;

import java.util.*;

/**
 * Organizes PlaceholderReadResultEntries by their starting offset and provides efficient methods for retrieving those
 * whose offsets are below certain values.
 */
class PlaceholderReadResultEntryCollection implements AutoCloseable {
    //region Members

    private final PriorityQueue<PlaceholderReadResultEntry> reads;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the PlaceholderReadResultEntryCollection class.
     */
    public PlaceholderReadResultEntryCollection() {
        this.reads = new PriorityQueue<>(this::entryComparator);
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
    public void add(PlaceholderReadResultEntry entry) {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        synchronized (this.reads) {
            this.reads.add(entry);
        }
    }

    /**
     * Finds the Result Entries that have a starting offset before the given offset, removes them from the collection,
     * and returns them.
     *
     * @param offset The offset to query against.
     * @return
     */
    public Collection<PlaceholderReadResultEntry> pollEntriesWithOffsetLessThan(long offset) {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        LinkedList<PlaceholderReadResultEntry> result = new LinkedList<>();
        if (this.reads.size() > 0) {
            synchronized (this.reads) {
                // 'reads' is sorted by Starting Offset, in ascending order. As long as it is not empty and the
                // first entry overlaps the given offset by at least one byte, extract and return it.
                while (this.reads.size() > 0 && this.reads.peek().getStreamSegmentOffset() <= offset) {
                    result.add(this.reads.poll());
                }
            }
        }

        return result;
    }

    /**
     * Cancels all Reads in this collection..
     */
    public void cancelAll() {
        LinkedList<PlaceholderReadResultEntry> toCancel;
        synchronized (this.reads) {
            toCancel = new LinkedList<>(this.reads);
            this.reads.clear();
        }

        toCancel.forEach(PlaceholderReadResultEntry::cancel);
    }

    private int entryComparator(PlaceholderReadResultEntry e1, PlaceholderReadResultEntry e2) {
        if (e1.getStreamSegmentOffset() < e2.getStreamSegmentOffset()) {
            return -1;
        }
        else if (e1.getStreamSegmentOffset() > e2.getStreamSegmentOffset()) {
            return 1;
        }

        return 0;
    }

    //endregion
}
