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
package io.pravega.segmentstore.contracts.tables;

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import java.util.Collection;

/**
 * Defines an iteration result that is returned by the {@link AsyncIterator} when invoking {@link TableStore#keyIterator},
 * {@link TableStore#entryIterator} or {@link TableStore#entryDeltaIterator}.
 */
public interface IteratorItem<T> {
    /**
     * Gets an array that represents the current state of the iteration. This value can be used to to reinvoke
     * {@link TableStore#keyIterator} or {@link TableStore#entryDeltaIterator} if a previous iteration has been interrupted.
     * @return An {@link ArrayView} containing the serialized state.
     */
    BufferView getState();

    /**
     * Gets a Collection of items that are contained in this instance. The items in this list are not necessarily related
     * to each other, nor are they guaranteed to be in any particular order.
     *
     * @return Items contained in this instance (not necessarily related to each other or in order)
     */
    Collection<T> getEntries();
}
