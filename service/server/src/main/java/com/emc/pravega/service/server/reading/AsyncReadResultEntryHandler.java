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

import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryType;

import java.time.Duration;

/**
 * Defines an Entry Handler for an AsyncReadResultProcessor.
 */
public interface AsyncReadResultEntryHandler {
    /**
     * Determines whether the AsyncReadResultProcessor should request data for the given ReadResultEntryType and Offset.
     * <p/>
     * This method will only be invoked on those ReadResultEntries that do not currently have data available for consumption,
     * such as StorageReadResultEntry or FutureReadResultEntry.
     * <p/>
     * If a call to this method returns false, the AsyncReadResultProcessor, as well as the underlying ReadResult, will be closed.
     *
     * @param entryType           The Type of the ReadResultEntry to process.
     * @param streamSegmentOffset The offset in the StreamSegment where the current ReadResultEntry is at.
     * @return True if the content should be requested, false otherwise.
     */
    boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset);

    /**
     * Processes the given entry.
     *
     * @param entry The entry to process.
     * @return True if consumption of the ReadResult should continue, false otherwise.
     */
    boolean processEntry(ReadResultEntry entry);

    /**
     * This method is called whenever a retrieval error occurred on a ReadResultEntry. This is not an exception handler,
     * rather just a notification that an exception occurred. After this is called, the generating AsyncReadResultProcessor
     * will auto-close.
     *
     * @param entry The entry that caused the processing error (this may be null, depending on when the error occurred).
     * @param cause The error that triggered this.
     */
    void processError(ReadResultEntry entry, Throwable cause);

    /**
     * Gets a value indicating the timeout for requesting content.
     *
     * @return The timeout.
     */
    Duration getRequestContentTimeout();
}
