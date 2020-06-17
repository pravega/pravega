/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.server.containers.StreamSegmentContainerMetadata;
import javax.annotation.concurrent.NotThreadSafe;

import lombok.RequiredArgsConstructor;

/**
 * Builder for StreamSegmentContainerMetadata to aid in testing.
 */
@RequiredArgsConstructor
@NotThreadSafe
public class MetadataBuilder {
    private final int containerId;
    private int maxActiveSegmentCount = 1000;

    /**
     * Creates a new instance of the StreamSegmentContainerMetadata class with the values accumulated in this builder.
     *
     * @return The result.
     */
    public UpdateableContainerMetadata build() {
        return new StreamSegmentContainerMetadata(this.containerId, this.maxActiveSegmentCount);
    }

    /**
     * Creates a new instance of the StreamSegmentContainerMetadata class with the values accumulated in this builder
     * and attempts to cast it to the given type.
     *
     * @param <T> Type that should be returned.
     * @return The result.
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerMetadata> T buildAs() {
        StreamSegmentContainerMetadata result = new StreamSegmentContainerMetadata(this.containerId, this.maxActiveSegmentCount);
        return (T) result;
    }

    /**
     * Sets the value for MaxActiveSegmentCount.
     *
     * @param value The value to set.
     * @return This object.
     */
    public MetadataBuilder withMaxActiveSegmentCount(int value) {
        this.maxActiveSegmentCount = value;
        return this;
    }
}
