/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.writer;

import com.google.common.base.Preconditions;

/**
 * Represents the result of a Storage Flush Operation.
 */
class FlushResult {
    private long flushedBytes;
    private long mergedBytes;

    FlushResult() {
        this.flushedBytes = 0;
        this.mergedBytes = 0;
    }

    /**
     * Adds a number of flushedBytes.
     *
     * @param flushedBytes The value to add.
     * @return This object.
     */
    FlushResult withFlushedBytes(long flushedBytes) {
        Preconditions.checkArgument(flushedBytes >= 0, "flushedBytes must be a positive number.");
        this.flushedBytes += flushedBytes;
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
        this.mergedBytes += mergedBytes;
        return this;
    }

    /**
     * Adds the given FlushResult to this one.
     *
     * @param flushResult The flush result to add.
     * @return This object.
     */
    FlushResult withFlushResult(FlushResult flushResult) {
        this.flushedBytes += flushResult.flushedBytes;
        this.mergedBytes += flushResult.mergedBytes;
        return this;
    }

    /**
     * Gets a value indicating the total amount of data flushed, in bytes.
     *
     * @return The result.
     */
    long getFlushedBytes() {
        return this.flushedBytes;
    }

    /**
     * Gets a value indicating the total amount of data that was merged, in bytes.
     *
     * @return The result.
     */
    long getMergedBytes() {
        return this.mergedBytes;
    }

    @Override
    public String toString() {
        return String.format("Flushed = %d, Merged = %d", this.flushedBytes, this.mergedBytes);
    }
}
