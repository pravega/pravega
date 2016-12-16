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

package com.emc.pravega.service.server.reading;

import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * A ReadResultEntry that wraps an inner Entry, but allows for offset adjustment. Useful for returning read results
 * that point to Transaction Read Indices.
 */
class RedirectedReadResultEntry implements CompletableReadResultEntry {
    //region Members

    private final CompletableReadResultEntry baseEntry;
    private final long adjustedOffset;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RedirectedReadResultEntry class.
     *
     * @param baseEntry        The CompletableReadResultEntry to wrap.
     * @param offsetAdjustment The amount to adjust the offset by.
     */
    RedirectedReadResultEntry(CompletableReadResultEntry baseEntry, long offsetAdjustment) {
        Preconditions.checkNotNull(baseEntry, "baseEntry");
        this.baseEntry = baseEntry;
        this.adjustedOffset = baseEntry.getStreamSegmentOffset() + offsetAdjustment;
        Preconditions.checkArgument(this.adjustedOffset >= 0, "Given offset adjustment would result in a negative offset.");
    }

    //endregion

    //region ReadResultEntry Implementation

    @Override
    public long getStreamSegmentOffset() {
        return this.adjustedOffset;
    }

    @Override
    public int getRequestedReadLength() {
        return this.baseEntry.getRequestedReadLength();
    }

    @Override
    public ReadResultEntryType getType() {
        return this.baseEntry.getType();
    }

    @Override
    public CompletableFuture<ReadResultEntryContents> getContent() {
        return this.baseEntry.getContent();
    }

    @Override
    public void requestContent(Duration timeout) {
        this.baseEntry.requestContent(timeout);
    }

    @Override
    public void setCompletionCallback(CompletionConsumer completionCallback) {
        this.baseEntry.setCompletionCallback(completionCallback);
    }

    //endregion

    @Override
    public String toString() {
        return String.format("%s, AdjOffset = %d", this.baseEntry.toString(), this.adjustedOffset);
    }
}
