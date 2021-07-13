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

/**
 * {@link IteratorState} encapsulates classes that will need to capture and pass state during iteration of a TableSegment.
 */
public interface IteratorState {

    /**
     * When paired with a deserialization method in the implementing class, this allows us to encapsulate asynchronous
     * iteration state in a portable manner.
     *
     * @return An {@link ArrayView} based serialization of the IteratorState we are encapsulating.
     */
    ArrayView serialize();

}
