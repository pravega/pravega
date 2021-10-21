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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import io.pravega.segmentstore.storage.SegmentHandle;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Helps read Segment data directly from Storage.
 */
public final class StreamSegmentStorageReader {
    /**
     * Reads a range of bytes from a Segment in Storage.
     *
     * @param segmentInfo   A SegmentProperties describing the Segment to read.
     * @param startOffset   The first offset within the Segment to read from.
     * @param maxReadLength The maximum number of bytes to read.
     * @param readBlockSize The maximum number of bytes to read at once (the returned StreamSegmentReadResult will be
     *                      broken down into Entries smaller than or equal to this size).
     * @param storage       A ReadOnlyStorage to execute the reads against.
     * @return A StreamSegmentReadResult that can be used to process the data. This will be made up of ReadResultEntries
     * of the following types: Storage, Truncated or EndOfSegment.
     */
    public static StreamSegmentReadResult read(SegmentProperties segmentInfo, long startOffset, int maxReadLength, int readBlockSize, ReadOnlyStorage storage) {
        Exceptions.checkArgument(startOffset >= 0, "startOffset", "startOffset must be a non-negative number.");
        Exceptions.checkArgument(maxReadLength >= 0, "maxReadLength", "maxReadLength must be a non-negative number.");
        Preconditions.checkNotNull(segmentInfo, "segmentInfo");
        Preconditions.checkNotNull(storage, "storage");
        String traceId = String.format("Read[%s]", segmentInfo.getName());
        return new StreamSegmentReadResult(startOffset, maxReadLength, new SegmentReader(segmentInfo, null, readBlockSize, storage), traceId);
    }

    /**
     * Reads a range of bytes from a Segment in Storage.
     *
     * @param handle        A SegmentHandle pointing to the Segment to read from.
     * @param startOffset   The first offset within the Segment to read from.
     * @param maxReadLength The maximum number of bytes to read.
     * @param readBlockSize The maximum number of bytes to read at once (the returned StreamSegmentReadResult will be
     *                      broken down into Entries smaller than or equal to this size).
     * @param storage       A ReadOnlyStorage to execute the reads against.
     * @return A StreamSegmentReadResult that can be used to process the data. This will be made up of ReadResultEntries
     * of the following types: Storage, Truncated or EndOfSegment.
     */
    public static StreamSegmentReadResult read(SegmentHandle handle, long startOffset, int maxReadLength, int readBlockSize, ReadOnlyStorage storage) {
        Exceptions.checkArgument(startOffset >= 0, "startOffset", "startOffset must be a non-negative number.");
        Exceptions.checkArgument(maxReadLength >= 0, "maxReadLength", "maxReadLength must be a non-negative number.");
        Preconditions.checkNotNull(handle, "handle");
        Preconditions.checkNotNull(storage, "storage");
        String traceId = String.format("Read[%s]", handle.getSegmentName());

        // Build a SegmentInfo using the information we are given. If startOffset or length are incorrect, the underlying
        // ReadOnlyStorage will throw appropriate exceptions at the caller.
        StreamSegmentInformation segmentInfo = StreamSegmentInformation.builder().name(handle.getSegmentName())
                .startOffset(startOffset)
                .length(startOffset + maxReadLength)
                .build();
        return new StreamSegmentReadResult(startOffset, maxReadLength, new SegmentReader(segmentInfo, handle, readBlockSize, storage), traceId);
    }

    //region SegmentReader

    /**
     * Helper class responsible with constructing the individual StreamSegmentReadResult entries.
     */
    private static class SegmentReader implements StreamSegmentReadResult.NextEntrySupplier {
        private final SegmentProperties segmentInfo;
        private final int readBlockSize;
        private final ReadOnlyStorage storage;
        private final AtomicReference<SegmentHandle> handle;

        SegmentReader(SegmentProperties segmentInfo, SegmentHandle handle, int readBlockSize, ReadOnlyStorage storage) {
            this.segmentInfo = segmentInfo;
            this.readBlockSize = readBlockSize;
            this.storage = storage;
            this.handle = new AtomicReference<>(handle);
        }

        @Override
        public CompletableReadResultEntry apply(Long readOffset, Integer readLength, Boolean makeCopyIgnored) {
            if (readOffset < this.segmentInfo.getStartOffset()) {
                // We attempted to read from a truncated portion of the Segment.
                return new TruncatedReadResultEntry(readOffset, readLength, this.segmentInfo.getStartOffset(), this.segmentInfo.getName());
            } else if (readOffset >= this.segmentInfo.getLength()) {
                // We've reached the end of a Sealed Segment.
                return new EndOfStreamSegmentReadResultEntry(readOffset, readLength);
            } else {
                // Execute the read from Storage.
                if (readOffset + readLength > segmentInfo.getLength()) {
                    // Adjust max read length to not exceed the last known offset of the Segment.
                    readLength = (int) (segmentInfo.getLength() - readOffset);
                }

                readLength = Math.min(this.readBlockSize, readLength);
                return new StorageReadResultEntry(readOffset, readLength, this::fetchContents);
            }
        }

        private void fetchContents(long segmentOffset, int readLength, Consumer<BufferView> successCallback,
                                   Consumer<Throwable> failureCallback, Duration timeout) {
            try {
                byte[] readBuffer = new byte[readLength];
                getHandle()
                        .thenCompose(h -> this.storage.read(h, segmentOffset, readBuffer, 0, readLength, timeout))
                        .thenAccept(bytesRead -> successCallback.accept(toReadResultEntry(readBuffer, bytesRead)))
                        .exceptionally(ex -> {
                            // Async failure.
                            Callbacks.invokeSafely(failureCallback, ex, null);
                            return null;
                        });
            } catch (Throwable ex) {
                // Synchronous failure.
                if (!Exceptions.mustRethrow(ex)) {
                    Callbacks.invokeSafely(failureCallback, ex, null);
                }

                throw ex;
            }
        }

        private BufferView toReadResultEntry(byte[] readBuffer, int size) {
            return new ByteArraySegment(readBuffer, 0, size);
        }

        private CompletableFuture<SegmentHandle> getHandle() {
            SegmentHandle h = this.handle.get();
            if (h != null) {
                return CompletableFuture.completedFuture(h);
            } else {
                return this.storage.openRead(this.segmentInfo.getName())
                        .thenApply(sh -> {
                            this.handle.set(sh);
                            return sh;
                        });
            }
        }
    }

    //endregion
}
