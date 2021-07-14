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
package io.pravega.segmentstore.server.tables;

import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.contracts.tables.TableSegmentInfo;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstraction for a general Table Segment Layout.
 */
@Slf4j
abstract class TableSegmentLayout implements AutoCloseable {
    //region Members
    /**
     * Default value used for when no offset is provided for a remove or put call.
     */
    protected static final long NO_OFFSET = -1;

    protected final Connector connector;
    protected final ScheduledExecutorService executor;
    protected final EntrySerializer serializer;
    protected final TableExtensionConfig config;
    protected final String traceObjectId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link TableSegmentLayout} class.
     *
     * @param connector A {@link Connector} that will be used to access the underlying Segments.
     * @param config    The {@link TableExtensionConfig} to use.
     * @param executor  A {@link ScheduledExecutorService} for async operations.
     */
    protected TableSegmentLayout(@NonNull Connector connector, @NonNull TableExtensionConfig config, @NonNull ScheduledExecutorService executor) {
        this.connector = connector;
        this.config = config;
        this.executor = executor;
        this.serializer = new EntrySerializer();
        this.traceObjectId = String.format("TableExtension[%s]", connector.getContainerId());
    }

    //endregion

    //region Abstract Methods

    @Override
    public abstract void close();

    /**
     * Creates any required {@link WriterTableProcessor} instances for the segment identified by the given metadata.
     *
     * @param metadata The Metadata for the Segment to create the {@link WriterTableProcessor}.
     * @return A Collection of {@link WriterTableProcessor} instances. If the collection is empty, no such processors are
     * required for this segment.
     */
    abstract Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata);

    /**
     * Get a collection of Segment Attributes to set on a newly created Table Segment with this layout.
     *
     * @param config The {@link TableSegmentConfig} for this segment.
     * @return A {@link Map} containing the attributes to set.
     */
    abstract Map<AttributeId, Long> getNewSegmentAttributes(@NonNull TableSegmentConfig config);

    /**
     * Deletes the given Table Segment.
     *
     * @param segmentName The name of the Table Segment.
     * @param mustBeEmpty If true, the Table Segment will only be deleted if empty.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the Table Segment has been deleted.
     */
    abstract CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout);

    /**
     * Inserts new or updates existing {@link TableEntry} instances into the given Table Segment.
     *
     * @param segment            A {@link DirectSegmentAccess} that represents the Table Segment to operate on.
     * @param entries            The entries to update. See {@link TableStore#put} for details.
     * @param tableSegmentOffset The expected offset of the TableSegment used for conditional (expected matches actual)
     *                           appends. See {@link TableStore#put} for details.
     * @param timer              A {@link TimeoutTimer} for the operation.
     * @return See {@link TableStore#put}.
     */
    abstract CompletableFuture<List<Long>> put(@NonNull DirectSegmentAccess segment, @NonNull List<TableEntry> entries, long tableSegmentOffset, TimeoutTimer timer);

    /**
     * Removes one or more {@link TableKey}s from the Table Segment.
     *
     * @param segment            A {@link DirectSegmentAccess} that represents the Table Segment to operate on.
     * @param keys               The keys to remove. See {@link TableStore#remove} for details.
     * @param tableSegmentOffset The expected offset of the TableSegment used for conditional (expected matches actual)
     *                           appends. See {@link TableStore#put} for details.
     * @param timer              A {@link TimeoutTimer} for the operation.
     * @return
     */
    abstract CompletableFuture<Void> remove(@NonNull DirectSegmentAccess segment, @NonNull Collection<TableKey> keys, long tableSegmentOffset, TimeoutTimer timer);

    /**
     * Looks up a List of {@link TableKey}s in the given Table Segment.
     *
     * @param segment A {@link DirectSegmentAccess} that represents the Table Segment to operate on.
     * @param keys    A List of {@link BufferView} instances representing the Keys to look up.
     * @param timer   A {@link TimeoutTimer} for the operation.
     * @return See {@link TableStore#get}.
     */
    abstract CompletableFuture<List<TableEntry>> get(@NonNull DirectSegmentAccess segment, @NonNull List<BufferView> keys, TimeoutTimer timer);

    /**
     * Creates a new Key Iterator. See {@link TableStore#keyIterator}.
     *
     * @param segment A {@link DirectSegmentAccess} that represents the Table Segment to operate on.
     * @param args    Arguments for the Iterator.
     * @return See {@link TableStore#keyIterator}.
     */
    abstract CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args);

    /**
     * Creates a new Entry Iterator. See {@link TableStore#entryIterator}.
     *
     * @param segment A {@link DirectSegmentAccess} that represents the Table Segment to operate on.
     * @param args    Arguments for the Iterator.
     * @return See {@link TableStore#entryIterator(String, IteratorArgs)}.
     */
    abstract CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args);

    /**
     * Creates a new Entry Iterator from the given position. See {@link TableStore#entryDeltaIterator}.
     *
     * @param segment      A {@link DirectSegmentAccess} that represents the Table Segment to operate on.
     * @param fromPosition The position to begin the iteration at.
     * @param fetchTimeout Timeout for each invocation of {@link AsyncIterator#getNext()}.
     * @return See {@link TableStore#entryDeltaIterator}.
     */
    abstract AsyncIterator<IteratorItem<TableEntry>> entryDeltaIterator(@NonNull DirectSegmentAccess segment, long fromPosition, Duration fetchTimeout);

    /**
     * Gets information about a Table Segment.
     *
     * @param segment A {@link DirectSegmentAccess} representing the segment to query.
     * @param timeout Timeout for the operation.
     * @return See {@link TableStore#getInfo}.
     */
    abstract CompletableFuture<TableSegmentInfo> getInfo(@NonNull DirectSegmentAccess segment, Duration timeout);

    //endregion

    //region Helpers

    /**
     * Normalizes deleted {@link TableEntry} instances.
     *
     * @param e The {@link TableEntry} to check.
     * @return Null if e is null or {@link TableEntry#getValue()} is null (for e), or e otherwise.
     */
    protected TableEntry maybeDeleted(TableEntry e) {
        return e == null || e.getValue() == null ? null : e;
    }

    protected void logRequest(String requestName, Object... args) {
        log.debug("{}: {} {}", this.traceObjectId, requestName, args);
    }

    //endregion

    //region Helper Classes

    @Data
    protected static class IteratorItemImpl<T> implements IteratorItem<T> {
        private final BufferView state;
        private final Collection<T> entries;
    }

    /**
     * Connector for the {@link TableSegmentLayout} and implementations.
     */
    @RequiredArgsConstructor
    public static final class Connector {
        /**
         * Container Id.
         */
        @Getter
        private final int containerId;
        /**
         * A {@link BiFunction} that will return a {@link DirectSegmentAccess} for the requested Segment Name.
         */
        @NonNull
        private final GetSegment getSegment;
        /**
         * A {@link BiFunction} that will delete the requested Segment.
         */
        @NonNull
        private final BiFunction<String, Duration, CompletableFuture<Void>> deleteSegment;

        protected CompletableFuture<DirectSegmentAccess> getSegment(String name, Duration timeout) {
            return getSegment(name, OperationPriority.Normal, timeout);
        }

        protected CompletableFuture<DirectSegmentAccess> getSegment(String name, OperationPriority priority, Duration timeout) {
            return this.getSegment.apply(name, priority, timeout);
        }

        protected CompletableFuture<Void> deleteSegment(String name, Duration timeout) {
            return this.deleteSegment.apply(name, timeout);
        }

        @FunctionalInterface
        public interface GetSegment {
            CompletableFuture<DirectSegmentAccess> apply(String segmentName, OperationPriority priority, Duration timeout);
        }
    }

    /**
     * Exception that is thrown whenever an Update/Removal batch is too large.
     */
    public static class UpdateBatchTooLargeException extends IllegalArgumentException {
        UpdateBatchTooLargeException(int length, int maxLength) {
            super(String.format("Update Batch length %s exceeds the maximum limit %s.", length, maxLength));
        }
    }

    //endregion
}
