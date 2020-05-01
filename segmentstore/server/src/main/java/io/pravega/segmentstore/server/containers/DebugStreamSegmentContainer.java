/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.DebugSegmentContainer;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class DebugStreamSegmentContainer extends StreamSegmentContainer implements DebugSegmentContainer {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private final ContainerConfig config;

    /**
     * Creates a new instance of the StreamSegmentContainer class.
     *
     * @param streamSegmentContainerId The Id of the StreamSegmentContainer.
     * @param config                   The ContainerConfig to use for this StreamSegmentContainer.
     * @param durableLogFactory        The DurableLogFactory to use to create DurableLogs.
     * @param readIndexFactory         The ReadIndexFactory to use to create Read Indices.
     * @param attributeIndexFactory    The AttributeIndexFactory to use to create Attribute Indices.
     * @param writerFactory            The WriterFactory to use to create Writers.
     * @param storageFactory           The StorageFactory to use to create Storage Adapters.
     * @param createExtensions         A Function that, given an instance of this class, will create the set of
     *                                 {@link SegmentContainerExtension}s to be associated with that instance.
     * @param executor                 An Executor that can be used to run async tasks.
     */
    DebugStreamSegmentContainer(int streamSegmentContainerId, ContainerConfig config, OperationLogFactory durableLogFactory,
                                ReadIndexFactory readIndexFactory, AttributeIndexFactory attributeIndexFactory,
                                WriterFactory writerFactory, StorageFactory storageFactory,
                                SegmentContainerFactory.CreateExtensions createExtensions, ScheduledExecutorService executor) {
        super(streamSegmentContainerId, config, durableLogFactory, readIndexFactory, attributeIndexFactory, writerFactory,
                storageFactory, createExtensions, executor);
        this.config = config;
    }

    /**
     * Creates a segment with given properties.
     * @param streamSegmentName         Name of the segment to be created.
     * @param length                    Length of the segment to be created.
     * @param isSealed                  Sealed status of the segment to be created.
     * @return                          A newly created segment.
     */
    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, long length, boolean isSealed
            /*TODO: pass in generic params */) {
        if (log.isDebugEnabled()) {
            log.debug("createStreamSegment called for {}", streamSegmentName);
        }
        StreamSegmentInformation segmentProp = StreamSegmentInformation.builder()
                .name(streamSegmentName)
                .length(length)
                .sealed(isSealed)
                .build();
        ArrayView segmentInfo = MetadataStore.SegmentInfo.serialize(MetadataStore.SegmentInfo.builder().properties(segmentProp).build());
        return metadataStore.createSegment(streamSegmentName, segmentInfo, new TimeoutTimer(TIMEOUT));
    }
}