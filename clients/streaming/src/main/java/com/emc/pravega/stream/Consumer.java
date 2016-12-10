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
package com.emc.pravega.stream;

import com.emc.pravega.stream.impl.EventReadImpl;

/**
 * A consumer for a stream.
 *
 * @param <T> The type of events being sent through this stream.
 */
public interface Consumer<T> extends AutoCloseable {

    /**
     * Gets the next event in the stream.
     *
     * @param timeout An upper bound on how long the call may block before returning null.
     * @return The next event in the stream, or null if timeout is reached.
     */
    EventRead<T> readNextEvent(long timeout);

    /**
     * Gets the configuration that this consumer was created with.
     */
    ConsumerConfig getConfig();

    /**
     * Re-read an event that was previously read, by passing the EventPointer returned from
     * {@link EventReadImpl#getEventPointer()}.
     * This is a blocking call.
     * 
     * @param pointer A pointer to the event to be returned.
     * @return The event at the position specified by the provided pointer or null if the event has
     *         been deleted.
     */
    T read(EventPointer pointer);

    /**
     * Close the consumer. No further actions may be performed. If this consumer is part of a
     * consumer group, this will automatically invoke
     * {@link ConsumerGroup#consumerOffline(String, Position)}
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
