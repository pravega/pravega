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
import java.util.function.Consumer;

/**
 * Read Result Entry for data that is not readily available in memory, but exists in Storage
 */
class StorageReadResultEntry extends ReadResultEntryBase {
    private final ContentFetcher contentFetcher;

    /**
     * Creates a new instance of the StorageReadResultEntry class.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     */
    StorageReadResultEntry(long streamSegmentOffset, int requestedReadLength, ContentFetcher contentFetcher) {
        super(ReadResultEntryType.Storage, streamSegmentOffset, requestedReadLength);
        Preconditions.checkNotNull(contentFetcher, "contentFetcher");
        this.contentFetcher = contentFetcher;
    }

    @Override
    public void requestContent(Duration timeout) {
        this.contentFetcher.accept(getStreamSegmentOffset(), getRequestedReadLength(), this::complete, this::fail,
                timeout);
    }

    @FunctionalInterface
    interface ContentFetcher {
        void accept(long streamSegmentOffset, int requestedReadLength, Consumer<ReadResultEntryContents>
                successCallback, Consumer<Throwable> failureCallback, Duration timeout);
    }
}
