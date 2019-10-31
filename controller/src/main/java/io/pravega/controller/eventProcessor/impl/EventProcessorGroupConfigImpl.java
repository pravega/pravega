/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor.impl;

import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

@Data
public class EventProcessorGroupConfigImpl implements EventProcessorGroupConfig {

    private final String streamName;

    private final String readerGroupName;

    private final int eventProcessorCount;

    private final CheckpointConfig checkpointConfig;

    @Builder
    private EventProcessorGroupConfigImpl(final String streamName,
                                         final String readerGroupName,
                                         final int eventProcessorCount,
                                         final CheckpointConfig checkpointConfig) {
        Preconditions.checkNotNull(streamName);
        Preconditions.checkNotNull(readerGroupName);
        Preconditions.checkArgument(!readerGroupName.contains("/"), "Invalid readerGroupName, '/' not allowed");
        Preconditions.checkArgument(eventProcessorCount > 0, "Event processor count should be positive integer");
        Preconditions.checkNotNull(checkpointConfig);
        this.streamName = streamName;
        this.readerGroupName = readerGroupName;
        this.eventProcessorCount = eventProcessorCount;
        this.checkpointConfig = checkpointConfig;
    }
}
