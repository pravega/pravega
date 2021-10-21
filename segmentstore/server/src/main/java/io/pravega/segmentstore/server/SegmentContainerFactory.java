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

    /**
     * Creates a new instance of a DebugSegmentContainer.
     *
     * @param containerId   The Id of the container to create.
     * @return              The DebugSegmentContainer instance.
     */
    DebugSegmentContainer createDebugStreamSegmentContainer(int containerId);

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
