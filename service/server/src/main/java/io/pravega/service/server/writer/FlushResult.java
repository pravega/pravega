/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.server.writer;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents the result of a Storage Flush Operation.
 */
class FlushResult {
    private AtomicLong flushedBytes;
    private AtomicLong mergedBytes;

    /**
     * Creates a new instance of the FlushResult class.
     */
    FlushResult() {
        this.flushedBytes = new AtomicLong();
        this.mergedBytes = new AtomicLong();
    }

    /**
     * Adds a number of flushedBytes.
     *
     * @param flushedBytes The value to add.
     * @return This object.
     */
    FlushResult withFlushedBytes(long flushedBytes) {
        Preconditions.checkArgument(flushedBytes >= 0, "flushedBytes must be a positive number.");
        this.flushedBytes.addAndGet(flushedBytes);
        return this;
    }

    /**
     * Adds a number of merged bytes.
     *
     * @param mergedBytes The value to add.
     * @return This object.
     */
    FlushResult withMergedBytes(long mergedBytes) {
        Preconditions.checkArgument(mergedBytes >= 0, "mergedBytes must be a positive number.");
        this.mergedBytes.addAndGet(mergedBytes);
        return this;
    }

    /**
     * Adds the given FlushResult to this one.
     *
     * @param flushResult The flush result to add.
     * @return This object.
     */
    FlushResult withFlushResult(FlushResult flushResult) {
        this.flushedBytes.addAndGet(flushResult.flushedBytes.get());
        this.mergedBytes.addAndGet(flushResult.mergedBytes.get());
        return this;
    }

    /**
     * Gets a value indicating the total amount of data flushed, in bytes.
     */
    long getFlushedBytes() {
        return this.flushedBytes.get();
    }

    /**
     * Gets a value indicating the total amount of data that was merged, in bytes.
     */
    long getMergedBytes() {
        return this.mergedBytes.get();
    }

    @Override
    public String toString() {
        return String.format("FlushedBytes = %s, MergedBytes = %s", this.flushedBytes, this.mergedBytes);
    }
}
