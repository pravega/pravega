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

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import java.util.Collection;

/**
 * Defines a Container for StreamSegments.
 */
public interface SegmentContainer extends StreamSegmentStore, Container {
    /**
     * Gets a collection of SegmentProperties for all active Segments (Active Segment = a segment that is currently allocated
     * in the internal Container's Metadata (usually a segment with recent activity)).
     *
     * @return A Collection with the SegmentProperties for these segments.
     */
    Collection<SegmentProperties> getActiveSegments();

    /**
     * Gets a registered {@link SegmentContainerExtension} of the given type.
     *
     * @param extensionClass Class of the {@link SegmentContainerExtension}.
     * @param <T>         Type of the {@link SegmentContainerExtension}.
     * @return A registered {@link SegmentContainerExtension} of the requested type.
     */
    <T extends SegmentContainerExtension> T getExtension(Class<T> extensionClass);
}
