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

import java.nio.ByteBuffer;

/**
 * Takes events being published and serializes them to byteBuffers so they can be sent over the wire
 * and deserializes these same byte buffers back into objects.
 * <p>
 * NOTE: There is no need for implementations to implement any sort of "framing"; the byte buffers
 * passed to deserialize will be of the appropriate length.
 *
 * @param <T> The type of event that this serializes.
 */
public interface Serializer<T> {
    /**
     * The maximum event size, in bytes.
     */
    int MAX_EVENT_SIZE = 1024 * 1024;

    /**
     * Serializes the given event.
     *
     * @param value The event to be serialized.
     * @return The serialized form of the event.
     * NOTE: buffers returned should not exceed {@link #MAX_EVENT_SIZE}.
     */
    ByteBuffer serialize(T value);

    /**
     * Deserializes the given ByteBuffer into an event.
     *
     * @param serializedValue A event that has been previously serialized.
     * @return The event object.
     */
    T deserialize(ByteBuffer serializedValue);
}
