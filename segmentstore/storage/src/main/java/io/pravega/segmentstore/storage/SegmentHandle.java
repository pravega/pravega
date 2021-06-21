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
package io.pravega.segmentstore.storage;

/**
 * Defines a Handle that can be used to operate on Segments in Storage.
 */
public interface SegmentHandle {
    /**
     * Gets the name of the Segment, as perceived by users of the Storage interface.
     */
    String getSegmentName();

    /**
     * Gets a value indicating whether this Handle was open in ReadOnly mode (true) or ReadWrite mode (false).
     */
    boolean isReadOnly();
}
