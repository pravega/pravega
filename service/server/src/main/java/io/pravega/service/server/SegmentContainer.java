/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server;

import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.contracts.StreamSegmentStore;

import java.util.Collection;

/**
 * Defines a Container for StreamSegments.
 */
public interface SegmentContainer extends StreamSegmentStore, Container {
    /**
     * Gets a collection of SegmentProperties for all active Segments (Active Segment = a segment that is currently allocated
     * in the internal Container's Metadata (usually a segment with recent activity)).
     *
     * @return A Collection with the SegmentProperties for these segments.
     */
    Collection<SegmentProperties> getActiveSegments();
}
