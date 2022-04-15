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
package io.pravega.client.stream;

import java.util.concurrent.ScheduledExecutorService;
/**
 * An event that was read from a stream or a checkpoint marker if one has been requested.
 * <p>
 * A checkpoint is an indication that the reading application should persist its state to durable storage
 * before reading further. A checkpoint also represents a point where events waiting to be read may be
 * rebalanced among the readers in a group. So before a checkpoint one reader might be handling all of the
 * events sent with routing key X but afterwards it may be a different reader.
 * 
 * @param <T> The type of the event.
 */
public interface EventRead<T> {
    
    /**
     * Returns the event that is wrapped in this EventRead or null a timeout occurred or if a checkpoint was requested.
     *
     * @return The event itself.
     */
    T getEvent();

    /**
     * The position in the stream that represents where the reader is immediately following this
     * event. It is useful to store this so that
     * {@link ReaderGroup#readerOffline(String, Position)} can be called if the reader dies.
     *
     * @return Position of the event
     */
    Position getPosition();

    /**
     * Returns a pointer object for the event read. The event pointer enables a random read of the
     * event at a future time.
     *
     * @return Pointer to an event
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
     * A boolean indicating if all events in the stream read completely.
     * This is true when the streams are sealed and all events in the streams have already been read.
     *
     * @return true if all events in the stream read completely.
     */
    boolean isReadCompleted();
    
    /**
     * If a checkpoint has been requested this will return the checkpointName passed to
     * {@link ReaderGroup#initiateCheckpoint(String,ScheduledExecutorService)} otherwise this will return null.
     * 
     * @return The name of the checkpoint
     */
    String getCheckpointName();
}
