/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import lombok.Data;

/**
 * Basic statistics for a Queue.
 */
@Data
public class QueueStats {
    public static final QueueStats DEFAULT = new QueueStats(0,0,0,0);
    /**
     * Gets the maximum degree of the Queue Processing parallelism.
     */
    private final int maxParallelism;

    /**
     * The number of items in the queue.
     */
    private final int size;

    /**
     * In case of a queue made up of data items, the average size of each item with respect to the maximum capacity for
     * that item. For example, if the maximum capacity for an item is 1MB and each item is about 700KB, then this
     * would be approx 0.7.
     */
    private final double averageItemFillRate;

    /**
     * The number of milliseconds elapsed since the oldest item presently in the queue has been added there.
     */
    private final int oldestItemTimeMillis;

    @Override
    public String toString() {
        return String.format("Size = %d, Fill = %.2f, OldestMillis = %d", this.size, this.averageItemFillRate, this.oldestItemTimeMillis);
    }
}
