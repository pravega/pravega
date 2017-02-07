/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.eventProcessor;

/**
 * Configuration for creating ActorGroup.
 */
public interface EventProcessorGroupConfig {

    /**
     * Gets name of stream which supplies events for Actors in the ActorGroup.
     * @return Stream name.
     */
    String getStreamName();

    /**
     * Gets name of ReaderGroup to which Actors in the ActorGroup belong.
     * @return ReaderGroup name.
     */
    String getReaderGroupName();

    /**
     * Gets the initial number of Actors in the ActorGroup.
     * @return Initial number of Actors.
     */
    int getActorCount();

    /**
     * Gets the frequency of persistence of Position objects by Actors in the ActorGroup.
     * @return Frequency of persistence of Position objects.
     */
    int getCheckpointFrequency();
}
