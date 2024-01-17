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

/**
 * Allows to read data from a specific segment.
 */
public interface SegmentReader<T> extends AutoCloseable {
    T read();

    Status checkStatus();

    /**
     * The status of this segment reader.
     */
    enum Status {
        /** The next event is available right now. */
        AVAILABLE_NOW,
        /** The segment is not sealed and reader has read all the events. */
        AVAILABLE_LATER,
        /** The segment is sealed and reader has read all the events. */
        FINISHED
    }
}
