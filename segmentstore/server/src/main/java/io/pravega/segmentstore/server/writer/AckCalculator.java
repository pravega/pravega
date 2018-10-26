/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.WriterSegmentProcessor;

/**
 * Helper class that calculates Ack Sequence Numbers based on input information from OperationProcessors.
 */
class AckCalculator {
    private final WriterState state;

    /**
     * Creates a new instance of the AckCalculator class.
     *
     * @param state The WriterState to use for calculation.
     */
    AckCalculator(WriterState state) {
        Preconditions.checkNotNull(state, "state");
        this.state = state;
    }

    /**
     * Determines the largest Sequence Number that can be safely truncated from the Writer's Data Source. All operations
     * up to, and including the one for this Sequence Number have been successfully committed to External Storage.
     * <p>
     * The Sequence Number we acknowledge has the property that all operations up to, and including it, have been
     * committed to Storage.
     * This can only be calculated by looking at all the active SegmentAggregators and picking the Lowest Uncommitted
     * Sequence Number (LUSN) among all of those Aggregators that have any outstanding data. The LUSN for each aggregator
     * has the property that, within the context of that Aggregator alone, all Operations that have a Sequence Number (SN)
     * smaller than LUSN have been committed to Storage. As such, picking the smallest of all LUSN values across
     * all the active SegmentAggregators will give us the highest SN that can be safely truncated out of the OperationLog.
     * Note that LUSN still points to an uncommitted Operation, so we need to subtract 1 from it to obtain the highest SN
     * that can be truncated up to (and including).
     * If we have no active Aggregators, then we have committed all operations that were passed to us, so we can
     * safely truncate up to LastReadSequenceNumber.
     *
     * @param processors The Processors to inspect for commit status.
     */
    <T extends WriterSegmentProcessor> long getHighestCommittedSequenceNumber(Iterable<T> processors) {
        long lowestUncommittedSeqNo = Long.MAX_VALUE;
        for (WriterSegmentProcessor a : processors) {
            if (!a.isClosed()) {
                long firstSeqNo = a.getLowestUncommittedSequenceNumber();
                if (firstSeqNo >= 0) {
                    // Subtract 1 from the computed LUSN and then make sure it doesn't exceed the LastReadSequenceNumber
                    // (it would only exceed it if there are no aggregators or of they are all empty - which means we processed everything).
                    lowestUncommittedSeqNo = Math.min(lowestUncommittedSeqNo, firstSeqNo - 1);
                }
            }
        }

        lowestUncommittedSeqNo = Math.min(lowestUncommittedSeqNo, this.state.getLastReadSequenceNumber());
        return lowestUncommittedSeqNo;
    }
}
