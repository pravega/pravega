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
package io.pravega.service.contracts;

import io.pravega.common.util.ImmutableDate;
import java.util.Map;
import java.util.UUID;

/**
 * General properties about a StreamSegment.
 */
public interface SegmentProperties {
    /**
     * Gets a value indicating the name of this StreamSegment.
     */
    String getName();

    /**
     * Gets a value indicating whether this StreamSegment is sealed for modifications.
     */
    boolean isSealed();

    /**
     * Gets a value indicating whether this StreamSegment is deleted (does not exist).
     */
    boolean isDeleted();

    /**
     * Gets a value indicating the last modification time of the StreamSegment.
     */
    ImmutableDate getLastModified();

    /**
     * Gets a value indicating the full, readable length of the StreamSegment.
     */
    long getLength();

    /**
     * Gets a read-only Map of AttributeId-Values for this Segment.
     *
     * @return The map.
     */
    Map<UUID, Long> getAttributes();
}

