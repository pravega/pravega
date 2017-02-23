/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
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
