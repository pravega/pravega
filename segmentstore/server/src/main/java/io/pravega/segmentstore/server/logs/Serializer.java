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
package io.pravega.segmentstore.server.logs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Defines an object that can serialize items to a Stream and deserialize them back.
 */
public interface Serializer<T> {
    /**
     * Serializes an item to the given OutputStream.
     *
     * @param output The OutputStream to serialize to.
     * @param item   The item to serialize.
     * @throws IOException If an exception occurred.
     */
    void serialize(OutputStream output, T item) throws IOException;

    /**
     * Deserializes an item from the given InputStream.
     *
     * @param input The InputStream to deserialize from.
     * @return The deserialized item.
     * @throws IOException If an exception occurred.
     */
    T deserialize(InputStream input) throws IOException;
}
