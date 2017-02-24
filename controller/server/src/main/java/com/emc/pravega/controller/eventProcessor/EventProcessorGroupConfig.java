/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor;

/**
 * Configuration for creating EventProcessorGroup.
 */
public interface EventProcessorGroupConfig {

    /**
     * Gets name of stream which supplies events for event processors in the EventProcessorGroup.
     * @return Stream name.
     */
    String getStreamName();

    /**
     * Gets name of ReaderGroup to which event processors in the EventProcessorGroup are subscribed.
     * @return ReaderGroup name.
     */
    String getReaderGroupName();

    /**
     * Gets the initial number of event processors in the EventProcessorGroup.
     * @return Initial number of event processors.
     */
    int getEventProcessorCount();

    /**
     * Gets the frequency of persistence of Position objects by event processors in the EventProcessorGroup.
     * @return Frequency of persistence of Position objects.
     */
    CheckpointConfig getCheckpointConfig();
}
