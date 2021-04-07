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
package io.pravega.segmentstore.server.logs.operations;

import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.server.SegmentOperation;
import java.util.Collection;

/**
 * Defines an Operation that can update a Segment's Attribute.
 */
public interface AttributeUpdaterOperation extends SegmentOperation {
    /**
     * Gets the Attribute updates for this StreamSegmentAppendOperation, if any.
     *
     * @return A Collection of Attribute updates, or null if no updates are available.
     */
    Collection<AttributeUpdate> getAttributeUpdates();

    /**
     * Gets a value indicating whether all {@link AttributeUpdate}s in {@link #getAttributeUpdates()} are Core Attributes.
     *
     * @return True if {@link #getAttributeUpdates()} is null or empty or has only Core Attributes, false otherwise.
     */
    default boolean hasOnlyCoreAttributes() {
        Collection<AttributeUpdate> updates = getAttributeUpdates();
        return updates == null
                || updates.isEmpty()
                || updates.stream().map(AttributeUpdate::getAttributeId).allMatch(Attributes::isCoreAttribute);
    }

    /**
     * Gets a value indicating whether this operation is marked as Internal. Such operations are generated by an internal
     * component (i.e., StorageWriter) and are not the result of an external action.
     *
     * @return True if internal, false otherwise.
     */
    default boolean isInternal() {
        return false;
    }
}
