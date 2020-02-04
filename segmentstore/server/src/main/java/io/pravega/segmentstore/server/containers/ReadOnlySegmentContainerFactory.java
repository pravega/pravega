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

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Represents a SegmentContainerFactory that builds instances of the ReadOnlySegmentContainer class.
 */
public class ReadOnlySegmentContainerFactory implements SegmentContainerFactory {
    public static final int READONLY_CONTAINER_ID = 0;
    private final StorageFactory storageFactory;
    private final ScheduledExecutorService executor;

    /**
     * Creates a new instance of the ReadOnlySegmentContainerFactory.
     *
     * @param storageFactory The Storage Factory to use for every container creation.
     * @param executor       The Executor to use for running async tasks.
     * @throws NullPointerException If any of the arguments are null.
     */
    public ReadOnlySegmentContainerFactory(StorageFactory storageFactory, ScheduledExecutorService executor) {
        this.storageFactory = Preconditions.checkNotNull(storageFactory, "storageFactory");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    @Override
    public SegmentContainer createStreamSegmentContainer(int containerId) {
        Preconditions.checkArgument(containerId == READONLY_CONTAINER_ID,
                "ReadOnly Containers can only have Id %s.", READONLY_CONTAINER_ID);
        return new ReadOnlySegmentContainer(this.storageFactory, this.executor);
    }
}
