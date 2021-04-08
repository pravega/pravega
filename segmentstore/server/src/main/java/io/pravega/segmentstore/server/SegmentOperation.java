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
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.contracts.SequencedElement;

/**
 * Defines a Log Operation that deals with a Segment.
 */
public interface SegmentOperation extends SequencedElement {
    /**
     * Gets a value indicating the Id of the StreamSegment this operation relates to.
     *
     * @return StreamSegment Id.
     */
    long getStreamSegmentId();
}
