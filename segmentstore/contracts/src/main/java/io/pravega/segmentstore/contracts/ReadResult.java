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
package io.pravega.segmentstore.contracts;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.util.BufferView;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a Read Result from a Stream Segment. This is essentially an Iterator over smaller, continuous ReadResultEntries.
 * Calls to hasNext() will return true only if the following conditions are all met:
 * <ul>
 * <li> The ReadResult is not closed </li>
 * <li> The ReadResult has not reached the end of a sealed StreamSegment </li>
 * <li> The ReadResult has not consumed all requested data (getConsumedLength()  {@literal <} getMaxResultLength()) </li>
 * </ul>
 * <p>
 * Calls to next() will return an element as long as hasNext() returns true. Some notes to consider:
 * <ul>
 * <li> next() will very likely not return the whole requested read range at the same time, even if subsequent calls to it
 * return contiguous data. </li>
 * <li> next() will return a specific instance of ReadResultEntry, depending on where the data to be read is located. </li>
 * <li> If next() returns an entry that has "isEndOfStreamSegment()" returning true, it means the Read Result has reached
 * the end of a sealed stream, and subsequent calls to hasNext() will return false. </li>
 * <li> If the data is readily available in memory, the returned ReadResultEntry will contain an already completed future,
 * ready for consumption (using the regular CompletableFuture methods). </li>
 * <li> If the data is not available in memory (currently), the returned ReadResultEntry will contain a CompletableFuture
 * that will be completed when data becomes available (pulled from Storage or Future Read). If the process of retrieving
 * data fails, the CompletableFuture will fail with the source exception as cause.</li>
 * <li> If the data requested is beyond the last offset of the StreamSegment (Future Read), the returned CompletableFuture
 * will complete when at least 1 byte of data from that offset is available, or will fail with a TimeoutException when the
 * timeout expires or StreamSegmentSealedException if the StreamSegment has been sealed at or before its offset. </li>
 * </ul>
 */
public interface ReadResult extends Iterator<ReadResultEntry>, AutoCloseable {
    /**
     * Gets a value indicating the Offset within the StreamSegment where this ReadResult starts at.
     *
     * @return  offset where the read result started at as a long
     */
    long getStreamSegmentStartOffset();

    /**
     * Gets a value indicating the maximum length that this read result can have.
     *
     * @return the max length possible for the read result
     */
    int getMaxResultLength();

    /**
     * Gets a value indicating the number of bytes that have been consumed via the next() method invocations.
     * Note that this does not track the individual consumption within the objects returned by next().
     *
     * @return number of bytes consumed via the next method invocation
     */
    int getConsumedLength();

    /**
     * Gets a value indicating whether "Copy-on-Read" is enabled for Cache retrievals. See {@link #setCopyOnRead} for
     * more details.
     *
     * @return True if copy-on-read is enabled for this {@link ReadResult}, false otherwise.
     */
    boolean isCopyOnRead();

    /**
     * Sets a value indicating whether "Copy-on-Read" is to be enabled for any Cache entry retrievals
     * ({@link ReadResultEntry#getType()} equals {@link ReadResultEntryType#Cache}). If true, then any data extracted
     * from the Cache will be copied into a new buffer (and thus decoupled from the cache). Use this option if you expect
     * your result to be used across requests or for longer periods of time, as this will prevent the {@link ReadResult}
     * from being invalidated in case of an eventual cache eviction.
     *
     * @param value True if enabling copy-on-read for this {@link ReadResult}, false otherwise.
     */
    void setCopyOnRead(boolean value);

    /**
     * Gets a value indicating the maximum number of bytes to read at once with every invocation of {@link #next()}.
     *
     * @return The maximum number of bytes to read at once.
     */
    int getMaxReadAtOnce();

    /**
     * Sets the maximum number of bytes to read at once with every invocation of {@link #next()}.
     *
     * @param value The value to set. If not positive or exceeds {@link #getMaxResultLength()}, this will be set to
     *              {@link #getMaxResultLength()}.
     */
    void setMaxReadAtOnce(int value);

    /**
     * Gets a value indicating whether this ReadResult is fully consumed (either because it was read in its entirety
     * or because it was closed externally).
     *
     * @return true if ReadResult is fully consumed or closed externally, otherwise false.
     */
    boolean isClosed();

    /**
     * Closes the ReadResult.
     */
    @Override
    void close();

    /**
     * Reads the remaining contents of the ReadResult into the given array. This will stop when the given target has been
     * filled or when the current end of the Segment has been reached.
     *
     * @param target       A byte array where the ReadResult will be read into.
     * @param fetchTimeout A timeout to use when needing to fetch the contents of an entry that is not in the Cache.
     * @return The number of bytes read.
     */
    @VisibleForTesting
    default int readRemaining(byte[] target, Duration fetchTimeout) {
        int bytesRead = 0;
        while (hasNext() && bytesRead < target.length) {
            ReadResultEntry entry = next();
            if (entry.getType() == ReadResultEntryType.EndOfStreamSegment || entry.getType() == ReadResultEntryType.Future) {
                // Reached the end.
                break;
            } else if (!entry.getContent().isDone()) {
                entry.requestContent(fetchTimeout);
            }

            BufferView contents = entry.getContent().join();
            int copied = contents.copyTo(ByteBuffer.wrap(target, bytesRead, Math.min(contents.getLength(), target.length - bytesRead)));
            bytesRead += copied;
        }

        return bytesRead;
    }

    /**
     * Reads the remaining contents of the ReadResult and returns a list of {@link BufferView}s that contain its contents.
     * This will stop when either the given maximum length or the end of the ReadResult has been reached.
     *
     * @param maxLength    The maximum number of bytes to read.
     * @param fetchTimeout A timeout to use when needing to fetch the contents of an entry that is not in the Cache.
     * @return A list of {@link BufferView}s with the data read.
     */
    default List<BufferView> readRemaining(int maxLength, Duration fetchTimeout) {
        int bytesRead = 0;
        ArrayList<BufferView> result = new ArrayList<>();
        while (hasNext() && bytesRead < maxLength) {
            ReadResultEntry entry = next();
            if (entry.getType() == ReadResultEntryType.EndOfStreamSegment || entry.getType() == ReadResultEntryType.Future) {
                // Reached the end.
                break;
            } else if (!entry.getContent().isDone()) {
                entry.requestContent(fetchTimeout);
            }
            result.add(entry.getContent().join());
        }
        return result;
    }
}
