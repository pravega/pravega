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

import com.emc.logservice.common.Exceptions;
import com.emc.logservice.contracts.ReadResultEntry;
import com.emc.logservice.contracts.ReadResultEntryContents;

import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletableFuture;

/**
 * Read Result Entry for data that is readily available for reading (in memory).
 */
public class MemoryReadResultEntry extends ReadResultEntry {
    private final CompletableFuture<ReadResultEntryContents> dataStream;

    /**
     * Creates a new instance of the MemoryReadResultEntry class.
     *
     * @param entry The ByteArrayReadIndexEntry to create the Result Entry from.
     * @throws IndexOutOfBoundsException If entryOffset, length or both are invalid.
     */
    public MemoryReadResultEntry(ByteArrayReadIndexEntry entry, int entryOffset, int length) {
        super(entry.getStreamSegmentOffset() + entryOffset, length);
        Exceptions.checkArgument(entryOffset >= 0, "entryOffset", "EntryOffset must be non-negative.");
        Exceptions.checkArgument(length > 0, "length", "Length must be a positive integer.");
        Exceptions.checkArgument(entryOffset + length <= entry.getLength(), "entryOffset + length", "EntryOffset + Length must be less than the size of the entry data.");

        // Data Stream is readily available.
        this.dataStream = CompletableFuture.completedFuture(new ReadResultEntryContents(new ByteArrayInputStream(entry.getData(), entryOffset, length), length));
    }

    @Override
    public CompletableFuture<ReadResultEntryContents> getContent() {
        return this.dataStream;
    }
}
