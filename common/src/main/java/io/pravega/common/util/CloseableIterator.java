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
package io.pravega.common.util;

import io.pravega.common.ObjectClosedException;

/**
 * Defines an Iterator that can be closed.
 * This can be used for such iterators that need to acquire or make use of expensive system resources, such as network
 * connections or file handles. Closing the iterator will release all such resources, even if getNext() indicates that
 * it hasn't reached the end.
 */
public interface CloseableIterator<T, TEx extends Exception> extends AutoCloseable {
    /**
     * Gets the next item in the iteration.
     *
     * @return The next item, or null if no more elements.
     * @throws ObjectClosedException If the CloseableIterator has been closed.
     * @throws TEx                   If an exception of this type occurred.
     */
    T getNext() throws TEx;

    /**
     * Closes the Iterator.
     */
    @Override
    void close();
}
