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
package io.pravega.segmentstore.contracts;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defines a set of Core Attributes.
 *
 * A Core Attribute is always present (if set) in the SegmentMetadata object. It is always serialized in
 * MetadataCheckpoints and in the Segment's State.
 *
 * An Extended Attribute is externally-defined and does not stick with the SegmentMetadata object. It is not part of
 * MetadataCheckpoints or the Segment's State, thus its value will not be reloaded into memory upon a failover or
 * segment eviction + re-activation.
 */
public class Attributes {
    /**
     * When used as an Attribute Value for a Boolean-like attribute, indicates a value equivalent to {@link Boolean#TRUE}.
     */
    public static final long BOOLEAN_TRUE = 1L;

    /**
     * When used as an Attribute Value for a Boolean-like attribute, indicates a value equivalent to {@link Boolean#FALSE}.
     */
    public static final long BOOLEAN_FALSE = 0L;

    /**
     * The Attribute ID at which Table Attributes can begin at. Everything with an ID smaller than this is a general
     * Attribute.
     */
    public static final long TABLE_ATTRIBUTES_START_OFFSET = 1024 * 1024; // Powers of 2 make UUID.toString look readable.

    /**
     * Defines an attribute value that denotes a missing value.
     */
    public static final long NULL_ATTRIBUTE_VALUE = Long.MIN_VALUE; //This is the same as WireCommands.NULL_ATTRIBUTE_VALUE

    /**
     * Prefix (Most Significant Bits) of the Id of all Core Attributes.
     */
    public static final long CORE_ATTRIBUTE_ID_PREFIX = Long.MIN_VALUE;

    /**
     * Defines an attribute that can be used to denote Segment creation time.
     */
    public static final AttributeId CREATION_TIME = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 0);

    /**
     * Defines an attribute that can be used to keep track of the number of events in a Segment.
     */
    public static final AttributeId EVENT_COUNT = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 1);

    /**
     * Defines an attribute that is used to keep scale policy type for stream segment.
     */
    public static final AttributeId SCALE_POLICY_TYPE = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 2);

    /**
     * Defines an attribute that is used to keep scale policy rate for stream segment.
     */
    public static final AttributeId SCALE_POLICY_RATE = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 3);

    /**
     * Defines an attribute that is used to set the value after which a Segment needs to be rolled over in Storage.
     */
    public static final AttributeId ROLLOVER_SIZE = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 4);

    /**
     * [Retired August 2018. Do not reuse as obsolete values may still linger around.]
     * Attribute Snapshot Location.
     */
    private static final AttributeId RETIRED_1 = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 5);

    /**
     * [Retired August 2018. Do not reuse as obsolete values may still linger around.]
     * Attribute Snapshot Length.
     */
    private static final AttributeId RETIRED_2 = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 6);

    /**
     * Defines an attribute that is used to keep a pointer (offset) to the Attribute Segment BTree Index Root Information.
     */
    public static final AttributeId ATTRIBUTE_SEGMENT_ROOT_POINTER = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 7);

    /**
     * Defines an attribute that is used to track the Sequence Number of the last Operation that was persisted into
     * the Attribute Index.
     */
    public static final AttributeId ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 8);

    /**
     * Defines an attribute that is used to store the Segment's Type ({@link SegmentType#getValue()}.
     * This attribute cannot be modified once set on the Segment.
     */
    public static final AttributeId ATTRIBUTE_SEGMENT_TYPE = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 9);

    /**
     * Defines an attribute that is used to store SLTS snapshot id.
     */
    public static final AttributeId ATTRIBUTE_SLTS_LATEST_SNAPSHOT_ID = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 10);

    /**
     * Defines an attribute that is used to store SLTS snapshot epoch.
     */
    public static final AttributeId ATTRIBUTE_SLTS_LATEST_SNAPSHOT_EPOCH = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 11);

    /**
     * Defines an attribute that is used to store the Segment's Extended Attribute Id Length.
     * If not specified (or if set to 0), the default will be the length of {@link AttributeId.UUID} (16 bytes).
     */
    public static final AttributeId ATTRIBUTE_ID_LENGTH = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 12);

    /**
     * Defines an attribute that is used to store the current epoch that the segment was created in. This is used to clean
     * up any transient segments that were not able to be cleaned up during normal shutdown/close procedures.
     */
    public static final AttributeId CREATION_EPOCH = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 15);

    /**
     * Defines an attribute that can be used to keep track of the event size that is allowed to be appended to index segment.
     */
    public static final AttributeId EXPECTED_INDEX_SEGMENT_EVENT_SIZE = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, 16);

    /**
     * Determines whether the given attribute cannot be modified once originally set on the Segment.
     *
     * @param attributeId The Attribute Id to check.
     * @return True if immutable, false otherwise.
     */
    public static boolean isUnmodifiable(AttributeId attributeId) {
        return attributeId == ATTRIBUTE_SEGMENT_TYPE || attributeId == ATTRIBUTE_ID_LENGTH;
    }

    /**
     * Determines whether the given Attribute Id refers to a Core Attribute.
     *
     * @param attributeId The Attribute Id to check.
     * @return True if Core Attribute, false otherwise.
     */
    public static boolean isCoreAttribute(AttributeId attributeId) {
        return attributeId.isUUID() && attributeId.getBitGroup(0) == CORE_ATTRIBUTE_ID_PREFIX;
    }

    /**
     * Returns a new Map of Attribute Ids to Values containing only those Core Attributes from the given Map that do not
     * have a null value.
     *
     * @param attributes The Map of Attribute Ids to Values to filter from.
     * @return A new Map containing only Core Attribute Ids and Values (from the original map).
     */
    public static Map<AttributeId, Long> getCoreNonNullAttributes(Map<AttributeId, Long> attributes) {
        return attributes.entrySet().stream()
                         .filter(e -> Attributes.isCoreAttribute(e.getKey()) && e.getValue() != NULL_ATTRIBUTE_VALUE)
                         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
