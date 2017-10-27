/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.AsyncMap;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.storage.Storage;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.Data;

/**
 * Combines information for a Segment stored in Storage (such as Length, Sealed, etc.) with information that is stored in
 * the State Store (such as attributes, Start Offset, Segment Id, etc.).
 */
class SegmentStateMapper {
    //region Members

    protected final Storage storage;
    private final AsyncMap<String, SegmentState> stateStore;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentStateMapper class.
     *
     * @param stateStore A AsyncMap that can be used to store Segment State.
     * @param storage    The Storage to use.
     */
    SegmentStateMapper(AsyncMap<String, SegmentState> stateStore, Storage storage) {
        this.stateStore = Preconditions.checkNotNull(stateStore, "stateStore");
        this.storage = Preconditions.checkNotNull(storage, "storage");
    }

    //endregion

    //region Operations

    /**
     * Gets information about a StreamSegment as it exists in Storage and in the State Store.
     *
     * @param streamSegmentName The case-sensitive StreamSegment Name.
     * @param timeout           Timeout for the Operation.
     * @return A CompletableFuture that, when complete, will contain a SegmentProperties object with the desired
     * information. If failed, it will contain the exception that caused the failure.
     */
    CompletableFuture<SegmentProperties> getSegmentInfoFromStorage(String streamSegmentName, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.storage
                .getStreamSegmentInfo(streamSegmentName, timer.getRemaining())
                .thenCompose(si -> attachState(si, timer.getRemaining()))
                .thenApply(si -> si.properties);
    }

    /**
     * Fetches a saved state (if any) for the given Segment.
     *
     * @param segmentName The Name of the Segment.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the result.
     */
    protected CompletableFuture<SegmentState> getState(String segmentName, Duration timeout) {
        return this.stateStore.get(segmentName, timeout);
    }

    /**
     * Fetches a saved state (if any) for the given source segment and returns a new SegmentInfo containing the same
     * information as the given source, but containing attributes fetched from the SegmentStateStore, as well as an updated
     * StartOffset.
     *
     * @param source  A SegmentProperties describing the Segment to fetch attributes for.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a SegmentInfo with the retrieved information.
     */
    protected CompletableFuture<SegmentInfo> attachState(SegmentProperties source, Duration timeout) {
        return getState(source.getName(), timeout)
                .thenApply(state -> {
                    if (state == null) {
                        // Nothing to change.
                        return new SegmentInfo(ContainerMetadata.NO_STREAM_SEGMENT_ID, source);
                    }

                    if (!source.getName().equals(state.getSegmentName())) {
                        throw new CompletionException(new DataCorruptionException(
                                String.format("Stored State for segment '%s' is corrupted. It refers to a different segment '%s'.",
                                        source.getName(),
                                        state.getSegmentName())));
                    }

                    SegmentProperties props = StreamSegmentInformation.from(source)
                            .attributes(state.getAttributes())
                            .startOffset(state.getStartOffset())
                            .build();
                    return new SegmentInfo(state.getSegmentId(), props);
                });
    }

    /**
     * Generates a new SegmentState based on the given Segment Information with given Attribute Updates and stores it in
     * the State Store.
     *
     * @param segmentInfo A SegmentProperties containing information about the Segment.
     * @param attributes  A Collection of AttributeUpdates to include in the State (may be null).
     * @param timeout     Timeout for the Operation.
     * @return A CompletableFuture that will be completed when the operation is done.
     */
    protected CompletableFuture<Void> saveState(SegmentProperties segmentInfo, Collection<AttributeUpdate> attributes, Duration timeout) {
        return this.stateStore.put(segmentInfo.getName(), composeState(segmentInfo, attributes), timeout);
    }

    /**
     * Returns a SegmentState for the given SegmentProperties, but with the given attribute updates applied.
     *
     * @param source           The base SegmentProperties to use.
     * @param attributeUpdates A collection of attribute updates to apply.
     * @return A SegmentState which contains the same information as source, but with applied attribute updates.
     */
    private SegmentState composeState(SegmentProperties source, Collection<AttributeUpdate> attributeUpdates) {
        if (attributeUpdates == null) {
            // Nothing to do.
            return new SegmentState(ContainerMetadata.NO_STREAM_SEGMENT_ID, source);
        }

        // Merge updates into the existing attributes.
        Map<UUID, Long> attributes = new HashMap<>(source.getAttributes());
        attributeUpdates.forEach(au -> attributes.put(au.getAttributeId(), au.getValue()));
        return new SegmentState(ContainerMetadata.NO_STREAM_SEGMENT_ID,
                StreamSegmentInformation.from(source).attributes(attributes).build());
    }


    @Data
    protected static class SegmentInfo {
        private final long segmentId;
        private final SegmentProperties properties;
    }

    //endregion
}
