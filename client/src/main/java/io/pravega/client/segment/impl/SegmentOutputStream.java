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
package io.pravega.client.segment.impl;

import io.pravega.client.stream.impl.PendingEvent;

import java.util.List;

/**
 * Defines an OutputStream for a segment.
 * Allows data to be appended to the end of the segment by calling {@link #write(PendingEvent)}
 */
public interface SegmentOutputStream extends AutoCloseable {

    /**
     * Returns the name of the segment associated to this output stream.
     *
     * @return The name of the segment associated to this output stream.
     */
    public abstract String getSegmentName();
    
    /**
     * Writes the provided data to the SegmentOutputStream.
     * 
     * The associated callback will be invoked when the operation is complete.
     * 
     * @param event The event to be added to the segment.
     */
    public abstract void write(PendingEvent event);

    /**
     * Flushes and then closes the output stream.
     * Frees any resources associated with it.
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    @Override
    public abstract void close() throws SegmentSealedException;

    /**
     * Block on all writes that have not yet completed.
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    public abstract void flush() throws SegmentSealedException;

    /**
     * This is meant to notify that an asynchronous flush call was called. Possibly a keep alive response
     * can be triggered.
     */
    public abstract void flushAsync();

    /**
     * Change the state of SegmentOutputStream to sealed to prevent future writes and return the list of unackedEvents.
     * This is invoked by the segmentSealed callback to fetch the unackedEvents to be resent to the right
     * SegmentOutputStreams.
     *
     * @return List of all the events that have been passed to write but have not yet been
     * acknowledged as written. The iteration order in the List is from oldest to newest.
     */
    public abstract List<PendingEvent> getUnackedEventsOnSeal();
    
    /**
     * This returns the write offset of a segment that was most recently observed from an Ack.
     * This may not be the same as the current write offset. 
     * If no acks have been observed on this segment it returns -1. 
     */
    public abstract long getLastObservedWriteOffset();
}