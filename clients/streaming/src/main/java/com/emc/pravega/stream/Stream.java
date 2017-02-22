/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

/**
 * A stream can be thought of as an infinite sequence of events.
 * A stream can be written to or read from.
 * A stream is:
 * Append only (Events in it are immutable once written)
 * Infinite (There are no limitations in size or time to how many events can go into a stream)
 * Strongly Consistent (Events are either in the stream or they are not, and not subject to reordering once written)
 * Scalable (The rate of events in a stream can greatly exceed the capacity of any single host)
 */
public interface Stream {
    /**
     * Gets the scope of this stream.
     *
     * @return String scope name
     */
    String getScope();

    /**
     * Gets the name of this stream  (Not including the scope).
     *
     * @return String a stream name
     */
    String getStreamName();

    /**
     * Gets the scoped name of this stream.
     *
     * @return String a fully scoped stream name
     */
    String getScopedName();

}
