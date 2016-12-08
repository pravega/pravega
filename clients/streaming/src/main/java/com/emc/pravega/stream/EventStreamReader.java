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

/**
 * A consumer for a stream.
 *
 * @param <T> The type of events being sent through this stream.
 */
public interface EventStreamReader<T> extends AutoCloseable {

    /**
     * Gets the next event in the stream.
     *
     * @param timeout An upper bound on how long the call may block before returning null.
     * @return The next event in the stream, or null if timeout is reached.
     */
    T readNextEvent(long timeout);

    /**
     * Gets the configuration that this consumer was created with.
     */
    ReaderConfig getConfig();

    /**
     * Gets an object that indicates the current position within the stream.
     */
    Position getPosition();

    /**
     * Reset the position of the consumer back to where it was previously.
     *
     * @param state The position previously obtained from {@link #getPosition()}
     */
    void setPosition(Position state);

    /**
     * Close the consumer. No further actions may be performed.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
