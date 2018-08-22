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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Defines a Factory for SegmentContainers.
 */
public interface SegmentContainerFactory {
    /**
     * A Function that returns an empty Map of Plugins.
     */
    CreatePlugins NO_PLUGINS = (container, executor) -> Collections.emptyMap();

    /**
     * Creates a new instance of a SegmentContainer.
     *
     * @param containerId The Id of the container to create.
     */
    SegmentContainer createStreamSegmentContainer(int containerId);

    @FunctionalInterface
    interface CreatePlugins {
        /**
         * Creates new instances of the SegmentContainerPlugins for the given Segment Container.
         *
         * @param container The SegmentContainer to create plugins for.
         * @param executor  An executor for async tasks.
         * @return A Map containing new instances of the SegmentContainerPlugins that were created, indexed by their c
         * class descriptors.
         */
        Map<Class<? extends SegmentContainerPlugin>, SegmentContainerPlugin> apply(SegmentContainer container, ScheduledExecutorService executor);
    }
}
