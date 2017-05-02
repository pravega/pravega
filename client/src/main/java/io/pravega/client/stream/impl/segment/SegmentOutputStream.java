/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream.impl.segment;

import io.pravega.client.stream.impl.PendingEvent;

import java.util.Collection;

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
     * Writes the provided data to the SegmentOutputStream. If
     * {@link PendingEvent#getExpectedOffset()} the data will be written only if the
     * SegmentOutputStream is currently of expectedLength.
     * 
     * The associated callback will be invoked when the operation is complete.
     * 
     * @param event The event to be added to the segment.
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    public abstract void write(PendingEvent event) throws SegmentSealedException;

    /**
     * Flushes and then closes the output stream.
     * Frees any resources associated with it.
     *
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    @Override
    public abstract void close() throws SegmentSealedException;

    /**
     * Block on all writes that have not yet completed.
     *
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    public abstract void flush() throws SegmentSealedException;

    /**
     * Returns a collection of all the events that have been passed to write but have not yet been
     * acknowledged as written. The iteration order in the collection is from oldest to newest.
     */
    public abstract Collection<PendingEvent> getUnackedEvents();
}