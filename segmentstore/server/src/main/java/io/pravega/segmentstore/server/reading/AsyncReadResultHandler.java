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
package io.pravega.segmentstore.server.reading;

import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import java.time.Duration;

/**
 * Defines an Entry Handler for an AsyncReadResultProcessor.
 */
public interface AsyncReadResultHandler {
    /**
     * Determines whether the AsyncReadResultProcessor should request data for the given ReadResultEntryType and Offset.
     *
     * This method will only be invoked on those ReadResultEntries that do not currently have data available for consumption,
     * such as StorageReadResultEntry or FutureReadResultEntry.
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
     * This method is called whenever an exception occurred while processing the ReadResult. This is not an exception handler,
     * rather just a notification that an exception occurred. After this is called, the generating AsyncReadResultProcessor
     * will auto-close.
     *
     * @param cause The error that triggered this.
     */
    void processError(Throwable cause);

    /**
     * This method is called when the AsyncReadResultProcessor terminates successfully or via a call to close().
     * If it terminates with an exception, then processError() will be invoked instead.
     * After this is called, the generating AsyncReadResultProcessor will auto-close.
     */
    void processResultComplete();

    /**
     * Gets a value indicating the timeout for requesting content.
     *
     * @return The timeout.
     */
    Duration getRequestContentTimeout();

    /**
     * Gets a value indicating the maximum number of bytes to process at any time. See {@link ReadResult#getMaxReadAtOnce()}.
     *
     * @return The maximum number of bytes to process at any time. Default value is {@link Integer#MAX_VALUE}, which
     * means the underlying read result has absolute freedom in choosing the read size.
     */
    default int getMaxReadAtOnce() {
        return Integer.MAX_VALUE;
    }
}
