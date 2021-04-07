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
package io.pravega.client.tables;

import io.pravega.client.tables.impl.IteratorStateImpl;
import java.nio.ByteBuffer;

/**
 * Represents the state of a resumable iterator. Such an iterator can be executed asynchronously and continued after an
 * interruption. Each iteration will result in a new request to the server (which is stateless). The entire state of
 * the iterator is encoded in this object and and is used by the server to decide what to return for the next iteration
 * call.
 * <p>
 * Each {@link IteratorState} instance is coupled to the Key-Value Table that it was created for and is tailored for the
 * type of iteration that generates it (i.e., {@link KeyValueTable#keyIterator} vs {@link KeyValueTable#entryIterator}).
 * As such, an {@link IteratorState} instance is non-transferable between different types of iterations or between
 * different Key-Value Tables. It is OK to pass a {@link IteratorState} generated from a {@link KeyValueTable} instance
 * to another {@link KeyValueTable} instance for the same Key-Value Table.
 */
public interface IteratorState {
    /**
     * Serializes this {@link IteratorState} to a {@link ByteBuffer}. This can later be used to create a new
     * {@link IteratorState} using {@link #fromBytes(ByteBuffer)}.
     *
     * @return A {@link ByteBuffer}.
     */
    ByteBuffer toBytes();

    /**
     * Gets a value indicating whether this {@link IteratorState} instance is empty.
     *
     * @return True if empty, false otherwise.
     */
    boolean isEmpty();

    /**
     * Creates a new {@link IteratorState} from the given {@link ByteBuffer}.
     *
     * @param buffer A {@link ByteBuffer} created via {@link #toBytes()}.
     * @return A {@link IteratorState}.
     */
    static IteratorState fromBytes(ByteBuffer buffer) {
        return IteratorStateImpl.fromBytes(buffer);
    }
}
