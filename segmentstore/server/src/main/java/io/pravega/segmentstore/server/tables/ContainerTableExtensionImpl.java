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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.BadSegmentTypeException;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.contracts.tables.TableSegmentInfo;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * A {@link ContainerTableExtension} that implements Table Segments on top of a {@link SegmentContainer}.
 */
@Slf4j
public class ContainerTableExtensionImpl implements ContainerTableExtension {
    //region Members

    private final SegmentContainer segmentContainer;
    private final ScheduledExecutorService executor;
    private final FixedKeyLengthTableSegmentLayout fixedKeyLayout;
    private final HashTableSegmentLayout hashTableLayout;
    private final AtomicBoolean closed;
    private final String traceObjectId;
    @Getter
    private final TableExtensionConfig config;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerTableExtensionImpl class.
     *
     * @param config           Configuration.
     * @param segmentContainer The {@link SegmentContainer} to associate with.
     * @param cacheManager     The {@link CacheManager} to use to manage the cache.
     * @param executor         An Executor to use for async tasks.
     */
    public ContainerTableExtensionImpl(TableExtensionConfig config, SegmentContainer segmentContainer, CacheManager cacheManager, ScheduledExecutorService executor) {
        this(config, segmentContainer, cacheManager, KeyHasher.sha256(), executor);
    }

    /**
     * Creates a new instance of the ContainerTableExtensionImpl class with custom {@link KeyHasher}.
     *
     * @param config           Configuration.
     * @param segmentContainer The {@link SegmentContainer} to associate with.
     * @param cacheManager     The {@link CacheManager} to use to manage the cache.
     * @param hasher           The {@link KeyHasher} to use.
     * @param executor         An Executor to use for async tasks.
     */
    @VisibleForTesting
    ContainerTableExtensionImpl(@NonNull TableExtensionConfig config, @NonNull SegmentContainer segmentContainer,
                                @NonNull CacheManager cacheManager, @NonNull KeyHasher hasher, @NonNull ScheduledExecutorService executor) {
        this.config = config;
        this.segmentContainer = segmentContainer;
        this.executor = executor;
        val connector = new TableSegmentLayout.Connector(this.segmentContainer.getId(), this.segmentContainer::forSegment, this.segmentContainer::deleteStreamSegment);
        this.hashTableLayout = new HashTableSegmentLayout(connector, cacheManager, hasher, this.config, this.executor);
        this.fixedKeyLayout = new FixedKeyLengthTableSegmentLayout(connector, this.config, this.executor);
        this.closed = new AtomicBoolean();
        this.traceObjectId = String.format("TableExtension[%d]", this.segmentContainer.getId());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.hashTableLayout.close();
            this.fixedKeyLayout.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region ContainerTableExtension Implementation

    @Override
    public Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (!metadata.getType().isTableSegment()) {
            // Not a Table Segment; nothing to do.
            return Collections.emptyList();
        }

        return selectLayout(metadata).createWriterSegmentProcessors(metadata);
    }

    private TableSegmentLayout selectLayout(SegmentMetadata segmentMetadata) {
        SegmentType type = segmentMetadata.getType();
        if (!type.isTableSegment()) {
            type = SegmentType.fromAttributes(segmentMetadata.getAttributes());
        }

        return selectLayout(segmentMetadata.getName(), type);
    }

    private TableSegmentLayout selectLayout(String segmentName, SegmentType segmentType) {
        if (segmentType.isFixedKeyLengthTableSegment()) {
            return this.fixedKeyLayout;
        } else if (segmentType.isTableSegment()) {
            return this.hashTableLayout;
        }

        throw new BadSegmentTypeException(segmentName, SegmentType.builder().tableSegment().build(), segmentType);
    }

    //endregion

    //region TableStore Implementation

    @Override
    public CompletableFuture<Void> createSegment(@NonNull String segmentName, SegmentType segmentType, TableSegmentConfig config, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        segmentType = SegmentType.builder(segmentType).tableSegment().build(); // Ensure at least a TableSegment type.
        val attributes = new HashMap<>(TableAttributes.DEFAULT_VALUES);
        attributes.putAll(selectLayout(segmentName, segmentType).getNewSegmentAttributes(config));
        val attributeUpdates = attributes
                .entrySet().stream()
                .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.None, e.getValue()))
                .collect(Collectors.toList());
        logRequest("createSegment", segmentName, segmentType, config);
        return this.segmentContainer.createStreamSegment(segmentName, segmentType, attributeUpdates, timeout);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        logRequest("deleteSegment", segmentName, mustBeEmpty);
        val timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> selectLayout(segment.getInfo()).deleteSegment(segmentName, mustBeEmpty, timer.getRemaining()), this.executor);
    }

    @Override
    public CompletableFuture<List<Long>> put(@NonNull String segmentName, @NonNull List<TableEntry> entries, Duration timeout) {
        return put(segmentName, entries, TableSegmentLayout.NO_OFFSET, timeout);
    }

    @Override
    public CompletableFuture<List<Long>> put(@NonNull String segmentName, @NonNull List<TableEntry> entries, long tableSegmentOffset, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> selectLayout(segment.getInfo()).put(segment, entries, tableSegmentOffset, timer), this.executor);
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, Duration timeout) {
        return remove(segmentName, keys, TableSegmentLayout.NO_OFFSET, timeout);
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, long tableSegmentOffset, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> selectLayout(segment.getInfo()).remove(segment, keys, tableSegmentOffset, timer), this.executor);
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(@NonNull String segmentName, @NonNull List<BufferView> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> selectLayout(segment.getInfo()).get(segment, keys, timer), this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.forSegment(segmentName, args.getFetchTimeout())
                .thenComposeAsync(segment -> selectLayout(segment.getInfo()).keyIterator(segment, args), this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.forSegment(segmentName, args.getFetchTimeout())
                .thenComposeAsync(segment -> selectLayout(segment.getInfo()).entryIterator(segment, args), this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryDeltaIterator(String segmentName, long fromPosition, Duration fetchTimeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.forSegment(segmentName, fetchTimeout)
                .thenApplyAsync(segment -> selectLayout(segment.getInfo()).entryDeltaIterator(segment, fromPosition, fetchTimeout), this.executor);
    }

    @Override
    public CompletableFuture<TableSegmentInfo> getInfo(String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        logRequest("getInfo", segmentName);
        val timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> selectLayout(segment.getInfo()).getInfo(segment, timer.getRemaining()), this.executor);
    }

    //endregion

    //region Helpers

    private void logRequest(String requestName, Object... args) {
        log.debug("{}: {} {}", this.traceObjectId, requestName, args);
    }

    //endregion

}
