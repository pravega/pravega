/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.Operation;

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
     *
     * The Sequence Number we acknowledge has the property that all operations up to, and including it, have been
     * committed to Storage.
     *
     * This can only be calculated by looking at all the active SegmentAggregators and picking the Lowest Uncommitted
     * Sequence Number (LUSN) among all of those Aggregators that have any outstanding data. The LUSN for each aggregator
     * has the property that, within the context of that Aggregator alone, all Operations that have a Sequence Number (SN)
     * smaller than LUSN have been committed to Storage. As such, picking the smallest of all LUSN values across
     * all the active SegmentAggregators will give us the highest SN that can be safely truncated out of the OperationLog.
     * Note that LUSN still points to an uncommitted Operation, so we need to subtract 1 from it to obtain the highest SN
     * that can be truncated up to (and including).
     *
     * If we have no active Aggregators, then we have committed all operations that were passed to us, so we can
     * safely truncate up to LastReadSequenceNumber.
     *
     * As opposed from {@link #getLowestUncommittedSequenceNumber}, this method should be called for
     * {@link WriterSegmentProcessor} instances that deal with different Segments.
     *
     * @param processors The {@link WriterSegmentProcessor} to inspect for commit status.
     * @param <T> {@link WriterSegmentProcessor} type.
     * @return The Highest Committed Sequence Number.
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

    /**
     * Determines the lowest Sequence Number across the given {@link WriterSegmentProcessor} instances that has not
     * yet been committed to Storage.
     * <p>
     * As opposed from {@link #getHighestCommittedSequenceNumber}, this method should be called for
     * {@link WriterSegmentProcessor} instances that deal with the same Segment.
     *
     * @param processors The {@link WriterSegmentProcessor} to inspect.
     * @param <T>        {@link WriterSegmentProcessor} type.
     * @return The Lowest Uncommited Sequence Number.
     */
    <T extends WriterSegmentProcessor> long getLowestUncommittedSequenceNumber(Iterable<T> processors) {
        // We need to find the lowest value across all processors that have uncommitted data.
        long result = Long.MAX_VALUE;
        for (WriterSegmentProcessor p : processors) {
            // If a processor has uncommitted data, its LUSN will be a positive number. Otherwise it means it is up-to-date.
            long sn = p.getLowestUncommittedSequenceNumber();
            if (sn >= 0 && sn < result) {
                result = sn;
            }
        }

        // If at least one processor has uncommitted data, return the smallest LUSN among them. Otherwise report that
        // everything is up-to date.
        return result == Long.MAX_VALUE ? Operation.NO_SEQUENCE_NUMBER : result;
    }
}
