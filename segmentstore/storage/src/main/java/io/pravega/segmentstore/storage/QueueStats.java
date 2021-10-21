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
package io.pravega.segmentstore.storage;

import lombok.Data;

/**
 * Basic statistics for a Queue.
 */
@Data
public class QueueStats {
    public static final QueueStats DEFAULT = new QueueStats(0, 0, 1024 * 1024, 0);
    /**
     * The number of items in the queue.
     */
    private final int size;

    /**
     * The total number of bytes outstanding in the queue.
     */
    private final long totalLength;

    /**
     * The maximum number of bytes for a single write in the queue.
     */
    private final int maxWriteLength;

    /**
     * The expected processing time for an item, in milliseconds.
     */
    private final int expectedProcessingTimeMillis;

    /**
     * Calculates the FillRatio, which is a number between [0, 1] that represents the average fill of each write with
     * respect to {@link #getMaxWriteLength()}.
     * For example, if {@link #getMaxWriteLength()} is 1MB and each item is about 700KB, then this would be approx 0.7.
     * @return The Average Item Fill Ratio.
     */
    public double getAverageItemFillRatio() {
        if (this.size > 0) {
            return Math.min(1, (double) this.totalLength / this.size / this.maxWriteLength);
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return String.format("Size = %d, Fill = %.2f, ProcTime = %dms", getSize(), getAverageItemFillRatio(), getExpectedProcessingTimeMillis());
    }
}
