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
package io.pravega.segmentstore.server.containers;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.Writer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Utility class that helps the {@link StreamSegmentContainer} to force-flush all the data to the underlying Storage.
 */
@RequiredArgsConstructor
@Slf4j
class LogFlusher {
    /**
     * Maximum number of {@link Writer} flushes to attempt until no more flush progress is expected to be made.
     */
    @VisibleForTesting
    static final int MAX_FLUSH_ATTEMPTS = 10;
    private final int containerId;
    @NonNull
    private final OperationLog durableLog;
    @NonNull
    private final Writer writer;
    @NonNull
    private final MetadataCleaner metadataCleaner;
    @NonNull
    private final ScheduledExecutorService executor;

    /**
     * Flushes every outstanding Operation in the Container's {@link OperationLog} to Storage. When this method completes:
     * - Every Operation that has been initiated in the {@link OperationLog} prior to the invocation of this method
     * will be flushed to the Storage via the {@link Writer}.
     * - The effects of such Operations on the Container's Metadata will be persisted to the Container's Metadata Store.
     * - The Container's Metadata Store will be persisted (and fully indexed) in Storage (it will contain all changes
     * from the previous step).
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed successfully. If the
     * operation failed, it will be failed with the appropriate exception.
     */
    public CompletableFuture<Void> flushToStorage(Duration timeout) {
        // 1. Flush everything we have so far.
        // 2. Flush all in-memory Segment metadata to the Metadata Store.
        // 3. Flush everything we have so far (again) - to make sure step 2 is persisted in Storage.
        TimeoutTimer timer = new TimeoutTimer(timeout);
        log.info("LogFlusher[{}]: Flushing outstanding data.", this.containerId);
        return flushAll(timer)
                .thenComposeAsync(v -> {
                    log.info("LogFlusher[{}]: Persisting active segment metadata.", this.containerId);
                    return this.metadataCleaner.persistAll(timer.getRemaining());
                }, this.executor)
                .thenComposeAsync(v -> {
                    log.info("LogFlusher[{}]: Flushing metadata store.", this.containerId);
                    return flushAll(timer);
                }, this.executor);
    }

    private CompletableFuture<Void> flushAll(TimeoutTimer timer) {
        // 1. Queue a checkpoint and get its SeqNo. This is poor man's way of ensuring all initiated ops are in.
        // 2. Tell StorageWriter to flush all (new API, with seqNo). Includes: segment data and table segment indexing.
        // 3. Repeat 1+2 until StorageWriter claims there is nothing more to flush.
        val flushAgain = new AtomicBoolean(true);
        val attemptNo = new AtomicInteger(0);
        return Futures.loop(
                () -> flushAgain.get() && attemptNo.getAndIncrement() < MAX_FLUSH_ATTEMPTS,
                () -> flushOnce(attemptNo.get(), timer),
                flushAgain::set,
                this.executor)
                .thenRun(() -> {
                    if (flushAgain.get()) {
                        throw new RetriesExhaustedException(new Exception(String.format("Unable to force-flush after %s attempts.", MAX_FLUSH_ATTEMPTS)));
                    }
                });
    }

    private CompletableFuture<Boolean> flushOnce(int attemptNo, TimeoutTimer timer) {
        return this.durableLog.checkpoint(timer.getRemaining())
                .thenComposeAsync(seqNo -> {
                    log.info("LogFlusher[{}]: Checkpointed at sequence number {}. Force-flushing to Storage ({}/{}).",
                            this.containerId, seqNo, attemptNo, MAX_FLUSH_ATTEMPTS);
                    return this.writer.forceFlush(seqNo, timer.getRemaining());
                }, this.executor);
    }
}
