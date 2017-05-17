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

import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Represents a SegmentContainerFactory that builds instances of the StreamSegmentContainer class.
 */
public class StreamSegmentContainerFactory implements SegmentContainerFactory {
    private final ContainerConfig config;
    private final OperationLogFactory operationLogFactory;
    private final ReadIndexFactory readIndexFactory;
    private final WriterFactory writerFactory;
    private final StorageFactory storageFactory;
    private final ScheduledExecutorService executor;

    /**
     * Creates a new instance of the StreamSegmentContainerFactory.
     *
     * @param config              The ContainerConfig to use for this StreamSegmentContainer.
     * @param operationLogFactory The OperationLogFactory to use for every container creation.
     * @param readIndexFactory    The ReadIndexFactory to use for every container creation.
     * @param writerFactory       The Writer Factory to use for every container creation.
     * @param storageFactory      The Storage Factory to use for every container creation.
     * @param executor            The Executor to use for running async tasks.
     * @throws NullPointerException If any of the arguments are null.
     */
    public StreamSegmentContainerFactory(ContainerConfig config, OperationLogFactory operationLogFactory, ReadIndexFactory readIndexFactory,
                                         WriterFactory writerFactory, StorageFactory storageFactory, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(operationLogFactory, "operationLogFactory");
        Preconditions.checkNotNull(readIndexFactory, "readIndexFactory");
        Preconditions.checkNotNull(writerFactory, "writerFactory");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.config = config;
        this.operationLogFactory = operationLogFactory;
        this.readIndexFactory = readIndexFactory;
        this.writerFactory = writerFactory;
        this.storageFactory = storageFactory;
        this.executor = executor;
    }

    @Override
    public SegmentContainer createStreamSegmentContainer(int containerId) {
        return new StreamSegmentContainer(containerId, config, this.operationLogFactory, this.readIndexFactory,
                this.writerFactory, this.storageFactory, this.executor);
    }
}
