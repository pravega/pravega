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

import io.pravega.client.stream.impl.PositionInternal;
import java.nio.ByteBuffer;

/**
 * A position in a stream. Used to indicate where a reader died. See {@link ReaderGroup#readerOffline(String, Position)}
 * Note that this is serializable so that it can be written to an external datastore.
 *
 */
public interface Position {
    
    /**
     * Used internally. Do not call.
     *
     * @return Implementation of position object interface
     */
    PositionInternal asImpl();
    
    /**
     * Serializes the position to a compact byte array.
     *
     * @return compact byte array
     */
    ByteBuffer toBytes();
    
    /**
     * Deserializes the position from its serialized from obtained from calling {@link #toBytes()}.
     * 
     * @param serializedPosition A serialized position.
     * @return The position object.
     */
    static Position fromBytes(ByteBuffer serializedPosition) {
        return PositionInternal.fromBytes(serializedPosition);
    }
}
