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

package com.emc.logservice.server.reading;

import com.emc.logservice.contracts.ReadResultEntry;
import com.emc.logservice.contracts.ReadResultEntryContents;

import java.util.concurrent.CompletableFuture;

/**
 * Read Result Entry for data that is not readily available in memory. This data may be in Storage, or it may be for an
 * offset that is beyond the StreamSegment's DurableLogLength.
 */
public class PlaceholderReadResultEntry extends ReadResultEntry {
    private final CompletableFuture<ReadResultEntryContents> result;

    /**
     * Creates a new instance of the PlaceholderReadResultEntry class.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     */
    public PlaceholderReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
        super(streamSegmentOffset, requestedReadLength);
        this.result = new CompletableFuture<>();
    }

    /**
     * Indicates that his placeholder read result entry can be completed with data that is now readily available.
     *
     * @param contents The contents of this read result.
     */
    protected void complete(ReadResultEntryContents contents) {
        this.result.complete(contents);
    }

    /**
     * Cancels this pending read result entry.
     */
    protected void cancel() {
        //TODO: this doesn't actually cancel the operation itself.
        this.result.cancel(true);
    }

    /**
     * Indicates that this placeholder read result entry cannot be fulfilled and is cancelled with the given exception as cause.
     *
     * @param cause The reason why the read was cancelled.
     */
    protected void fail(Throwable cause) {
        this.result.completeExceptionally(cause);
    }

    //region ReadResultEntry Implementation

    @Override
    public CompletableFuture<ReadResultEntryContents> getContent() {
        return result;
    }

    //endregion
}
