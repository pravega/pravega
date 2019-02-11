/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.io.StreamHelpers;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.SneakyThrows;

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
     *
     */
    int getConsumedLength();

    /**
     * Gets a value indicating whether this ReadResult is fully consumed (either because it was read in its entirety
     * or because it was closed externally).
     *
     * @return true if ReadResult  is fully consumed or closed externally, otherwise false
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
    @SneakyThrows(IOException.class)
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

            ReadResultEntryContents contents = entry.getContent().join();
            StreamHelpers.readAll(contents.getData(), target, bytesRead, Math.min(contents.getLength(), target.length - bytesRead));
            bytesRead += contents.getLength();
        }

        return bytesRead;
    }

    /**
     * Reads the remaining contents of the ReadResult and returns an ordered List of InputStreams that contain its contents.
     * This will stop when either the given maximum length or the end of the ReadResult has been reached.
     *
     * @param maxLength    The maximum number of bytes to read.
     * @param fetchTimeout A timeout to use when needing to fetch the contents of an entry that is not in the Cache.
     * @return A List containing InputStreams with the data read.
     */
    default List<InputStream> readRemaining(int maxLength, Duration fetchTimeout) {
        int bytesRead = 0;
        ArrayList<InputStream> result = new ArrayList<>();
        while (hasNext() && bytesRead < maxLength) {
            ReadResultEntry entry = next();
            if (entry.getType() == ReadResultEntryType.EndOfStreamSegment || entry.getType() == ReadResultEntryType.Future) {
                // Reached the end.
                break;
            } else if (!entry.getContent().isDone()) {
                entry.requestContent(fetchTimeout);
            }
            result.add(entry.getContent().join().getData());
        }
        return result;
    }
}
