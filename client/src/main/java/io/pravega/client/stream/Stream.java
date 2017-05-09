/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

/**
 * A stream can be thought of as an unbounded sequence of events.
 * A stream can be written to or read from.
 * A stream is:
 * Append only (Events in it are immutable once written)
 * Unbounded (There are no limitations in how many events can go into a stream)
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
