/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream;

/**
 * An event that was read from a stream or a checkpoint marker if one has been requested.
 * 
 * A checkpoint is an indication that the reading application should persist its state to durable storage
 * before reading further. 
 * 
 * @param <T> The type of the event.
 */
public interface EventRead<T> {
    
    /**
     * Returns the event or null if a checkpoint was requested.
     */
    T getEvent();

    /**
     * The position in the stream that represents where the reader is immediately following this
     * event. It is useful to store this so that
     * {@link ReaderGroup#readerOffline(String, Position)} can be called if the reader dies.
     */
    Position getPosition();

    /**
     * Returns a pointer object for the event read. The event pointer enables a random read of the
     * event at a future time.
     */
    EventPointer getEventPointer();

    /**
     * A boolean indicating if this is a checkpoint. In which case {@link #getCheckpointName()} will be non-null
     * and {@link #getEvent()} will be null.
     * 
     * @return true if this is a checkpoint.
     */
    boolean isCheckpoint();
    
    /**
     * If a checkpoint has been requested this will return the checkpointName passed to
     * {@link ReaderGroup#initiateCheckpoint(String)} otherwise this will return null.
     * 
     * @return The name of the checkpoint
     */
    String getCheckpointName();
}
