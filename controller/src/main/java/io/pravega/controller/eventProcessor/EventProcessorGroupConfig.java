/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor;

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
