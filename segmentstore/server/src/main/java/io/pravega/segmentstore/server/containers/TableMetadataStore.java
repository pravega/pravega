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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Runnables;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.tables.TableExtensionConfig;
import io.pravega.shared.NameUtils;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * {@link MetadataStore} implementation that stores all Segment Information as {@link TableEntry} instances in a dedicated
 * Table Segment per Segment Container.
 */
@Slf4j
class TableMetadataStore extends MetadataStore {
    //region Members
    private final TableStore tableStore;
    private final String metadataSegmentName;
    private final AtomicBoolean initialized;
    private final TableExtensionConfig config;
    //endregion

    /**
     * Creates a new instance of the {@link TableMetadataStore} class.
     *
     * @param connector  A {@link MetadataStore.Connector} object that can be used to communicate between the
     *                   {@link MetadataStore} and upstream callers.
     * @param tableStore A {@link TableStore} to use.
     * @param config     A {@link TableExtensionConfig} to use.
     * @param executor   The executor to use for async operations.
     */
    TableMetadataStore(Connector connector, @NonNull TableStore tableStore, @NonNull TableExtensionConfig config, Executor executor) {
        super(connector, executor);
        this.tableStore = tableStore;
        this.config = config;
        this.metadataSegmentName = NameUtils.getMetadataSegmentName(connector.getContainerMetadata().getContainerId());
        this.initialized = new AtomicBoolean(false);
    }

    //region MetadataStore Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        Preconditions.checkState(!this.initialized.get(), "TableMetadataStore is already initialized.");

        // Invoke submitAssignment(), which will ensure that the Metadata Segment is mapped in memory and pinned.
        // If this is the first time we initialize the TableMetadataStore for this SegmentContainer, a new id will be
        // assigned to it.
        val attributes = new HashMap<>(TableAttributes.DEFAULT_VALUES);
        attributes.putAll(this.config.getDefaultCompactionAttributes()); // Make sure we enable rollover for this segment.
        val attributeUpdates = attributes
                .entrySet().stream()
                .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.None, e.getValue()))
                .collect(Collectors.toList());

        // Container Metadata Segment is a System Table Segment. It is System, Internal, and Critical.
        val segmentType = SegmentType.builder().tableSegment().system().critical().internal().build();
        return submitAssignment(SegmentInfo.newSegment(this.metadataSegmentName, segmentType, attributeUpdates), true, timeout)
                .thenAccept(segmentId -> {
                    this.initialized.set(true);
                    log.info("{}: Metadata Segment pinned. Name = '{}', Id = '{}'", this.traceObjectId, this.metadataSegmentName, segmentId);
                });
    }

    @Override
    public CompletableFuture<Void> recover(SegmentProperties segmentProperties, Duration timeout) {
        Preconditions.checkState(!this.initialized.get(), "TableMetadataStore is already initialized.");

        log.info("container metadata segment {} recovered with length {}", segmentProperties.getName(), segmentProperties.getLength());
        val attributes = new HashMap<>(TableAttributes.DEFAULT_VALUES);
        attributes.putAll(this.config.getDefaultCompactionAttributes()); // Make sure we enable rollover for this segment.
        attributes.put(TableAttributes.INDEX_OFFSET, segmentProperties.getLength());
        val segmentType = SegmentType.builder().tableSegment().system().critical().internal().build();
        attributes.put(Attributes.ATTRIBUTE_SEGMENT_TYPE, segmentType.getValue());

        StreamSegmentInformation properties = StreamSegmentInformation.builder()
                .length(segmentProperties.getLength())
                .name(segmentProperties.getName())
                .sealed(segmentProperties.isSealed())
                .deleted(segmentProperties.isDeleted())
                .startOffset(segmentProperties.getStartOffset())
                .storageLength(segmentProperties.getLength())
                .deletedInStorage(segmentProperties.isDeleted())
                .lastModified(segmentProperties.getLastModified())
                .attributes(attributes)
                .build();

        SegmentInfo info = SegmentInfo.builder()
                .segmentId(ContainerMetadata.NO_STREAM_SEGMENT_ID)
                .properties(properties)
                .build();

        return submitAssignment(info, true, timeout)
                .thenAccept(segmentId -> {
                    this.initialized.set(true);
                    log.info("{}: Metadata Segment pinned. Name = '{}', Id = '{}'", this.traceObjectId, this.metadataSegmentName, segmentId);
                });
    }

    @Override
    CompletableFuture<Void> createSegment(String segmentName, SegmentType segmentType, Collection<AttributeUpdate> attributes, Duration timeout) {
        // Make sure we don't try to create the Metadata Segment - it is reserved.
        ensureInitialized();
        Preconditions.checkArgument(!this.metadataSegmentName.equals(segmentName),
                "Cannot create Metadata Segment if already initialized.");
        return super.createSegment(segmentName, segmentType, attributes, timeout);
    }

    @Override
    protected CompletableFuture<Void> createSegment(String segmentName, ArrayView segmentInfo, TimeoutTimer timer) {
        ensureInitialized();
        TableEntry entry = TableEntry.notExists(getTableKey(segmentName), segmentInfo);
        return this.tableStore
                .put(this.metadataSegmentName, Collections.singletonList(entry), timer.getRemaining())
                .handle((ignored, ex) -> {
                    if (ex != null) {
                        if (Exceptions.unwrap(ex) instanceof BadKeyVersionException) {
                            ex = new StreamSegmentExistsException(segmentName);
                        }
                        throw new CompletionException(ex);
                    }
                    return null;
                });
    }

    @Override
    public CompletableFuture<Boolean> clearSegmentInfo(String segmentName, Duration timeout) {
        return applyToSegment(
                segmentName,
                (entry, t2) -> this.tableStore
                        .remove(this.metadataSegmentName, Collections.singleton(TableKey.unversioned(entry.getKey().getKey())), t2)
                        .thenApply(v -> true),
                () -> CompletableFuture.completedFuture(false),
                timeout);
    }

    @Override
    protected CompletableFuture<BufferView> getSegmentInfoInternal(String segmentName, Duration timeout) {
        return applyToSegment(
                segmentName,
                (entry, t2) -> CompletableFuture.completedFuture(entry.getValue()),
                () -> Futures.failedFuture(new StreamSegmentNotExistsException(segmentName)),
                timeout);
    }

    private <T> CompletableFuture<T> applyToSegment(String segmentName, BiFunction<TableEntry, Duration, CompletableFuture<T>> ifExists,
                                                    Supplier<CompletableFuture<T>> ifNotExists, Duration timeout) {
        ensureInitialized();
        ArrayView key = getTableKey(segmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.tableStore
                .get(this.metadataSegmentName, Collections.singletonList(key), timer.getRemaining())
                .thenComposeAsync(existingData -> {
                    assert existingData.size() == 1 : "Expecting only one result";
                    if (existingData.get(0) == null) {
                        // We don't know anything about this Segment.
                        return ifNotExists.get();
                    }

                    // We have an entry.
                    return ifExists.apply(existingData.get(0), timer.getRemaining());
                }, this.executor);
    }

    @Override
    protected CompletableFuture<Void> updateSegmentInfo(String segmentName, ArrayView segmentInfo, Duration timeout) {
        ensureInitialized();
        TableEntry entry = TableEntry.unversioned(getTableKey(segmentName), segmentInfo);
        return this.tableStore.put(this.metadataSegmentName, Collections.singletonList(entry), timeout).thenRun(Runnables.doNothing());
    }

    private void ensureInitialized() {
        Preconditions.checkState(this.initialized.get(), "TableMetadataStore is not initialized.");
    }

    private ArrayView getTableKey(String segmentName) {
        return new ByteArraySegment(segmentName.getBytes(Charsets.UTF_8));
    }
}
