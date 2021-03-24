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
package io.pravega.segmentstore.server;

import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents the result of a Storage Flush Operation.
 */
public class WriterFlushResult {
    private final AtomicLong flushedBytes;
    private final AtomicLong mergedBytes;
    private final AtomicInteger flushedAttributes;

    /**
     * Creates a new instance of the WriterFlushResult class.
     */
    public WriterFlushResult() {
        this.flushedBytes = new AtomicLong();
        this.mergedBytes = new AtomicLong();
        this.flushedAttributes = new AtomicInteger();
    }

    /**
     * Adds a number of flushedBytes.
     *
     * @param flushedBytes The value to add.
     * @return This object.
     */
    public WriterFlushResult withFlushedBytes(long flushedBytes) {
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
    public WriterFlushResult withMergedBytes(long mergedBytes) {
        Preconditions.checkArgument(mergedBytes >= 0, "mergedBytes must be a positive number.");
        this.mergedBytes.addAndGet(mergedBytes);
        return this;
    }

    /**
     * Adds a number of flushed attributes.
     *
     * @param flushedAttributes The value to add.
     * @return This object.
     */
    public WriterFlushResult withFlushedAttributes(int flushedAttributes) {
        Preconditions.checkArgument(flushedAttributes >= 0, "flushedAttributes must be a positive number.");
        this.flushedAttributes.addAndGet(flushedAttributes);
        return this;
    }

    /**
     * Adds the given WriterFlushResult to this one.
     *
     * @param flushResult The flush result to add.
     * @return This object.
     */
    public WriterFlushResult withFlushResult(WriterFlushResult flushResult) {
        this.flushedBytes.addAndGet(flushResult.flushedBytes.get());
        this.mergedBytes.addAndGet(flushResult.mergedBytes.get());
        this.flushedAttributes.addAndGet(flushResult.flushedAttributes.get());
        return this;
    }

    /**
     * Gets a value indicating the total amount of data flushed, in bytes.
     * @return The total amount (bytes) of data flushed.
     */
    public long getFlushedBytes() {
        return this.flushedBytes.get();
    }

    /**
     * Gets a value indicating the total amount of data that was merged, in bytes.
     * @return The total amount (bytes) of data merged.
     */
    public long getMergedBytes() {
        return this.mergedBytes.get();
    }

    /**
     * Gets a value indicating the number of attributes flushed.
     *
     * @return The number of attributes flushed.
     */
    public int getFlushedAttributes() {
        return this.flushedAttributes.get();
    }

    /**
     * Gets a value indicating whether anything was flushed (data, attributes, etc.).
     *
     * @return True if anything was flushed, false otherwise.
     */
    public boolean isAnythingFlushed() {
        return getFlushedAttributes() > 0 || getFlushedBytes() > 0 || getMergedBytes() > 0;
    }

    @Override
    public String toString() {
        return String.format("FlushedBytes = %s, MergedBytes = %s, Attributes = %s", this.flushedBytes, this.mergedBytes, this.flushedAttributes);
    }
}
