/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Defines a Factory for SegmentContainers.
 */
public interface SegmentContainerFactory {
    /**
     * Creates a new instance of a SegmentContainer.
     *
     * @param containerId The Id of the container to create.
     * @return The SegmentContainer instance.
     */
    SegmentContainer createStreamSegmentContainer(int containerId);

    @FunctionalInterface
    interface CreateExtensions {
        /**
         * Creates new instances of the {@link SegmentContainerExtension}s for the given {@link SegmentContainer}.
         *
         * @param container The {@link SegmentContainer} to create Extensions for.
         * @param executor  A {@link ScheduledExecutorService} for async tasks.
         * @return A Map containing new instances of the {@link SegmentContainerExtension}s that were created, indexed
         * by their class descriptors.
         */
        Map<Class<? extends SegmentContainerExtension>, SegmentContainerExtension> apply(SegmentContainer container, ScheduledExecutorService executor);
    }
}
