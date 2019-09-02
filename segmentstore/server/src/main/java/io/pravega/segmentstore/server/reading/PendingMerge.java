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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Holds information about a pending Segment merge.
 */
@ThreadSafe
@RequiredArgsConstructor
class PendingMerge {
    /**
     * The offset in the Target Segment where the merge is executed at.
     */
    @Getter
    private final long mergeOffset;
    @GuardedBy("this")
    private final List<FutureReadResultEntry> reads = new ArrayList<>();
    @GuardedBy("this")
    private boolean sealed = false;

    /**
     * Seals this PendingMerge instance and returns a list of all the registered FutureReadResultEntries associated with it.
     * After this method returns, all calls to register() will return false.
     * @return A List of all the registered FutureReadResultEntries recorded.
     */
    synchronized List<FutureReadResultEntry> seal() {
        this.sealed = true;
        List<FutureReadResultEntry> result = new ArrayList<>(this.reads);
        this.reads.clear();
        return result;
    }

    /**
     * Registers the given FutureReadResultEntry into this PendingMerge.
     * @param entry The entry to register.
     * @return True if the entry was registered (i.e., seal() was not invoked), false otherwise.
     */
    synchronized boolean register(FutureReadResultEntry entry) {
        if (!this.sealed) {
            this.reads.add(entry);
        }

        return !this.sealed;
    }

    @Override
    public synchronized String toString() {
        return String.format("Offset = %d, ReadCount = %d", this.mergeOffset, this.reads.size());
    }
}
