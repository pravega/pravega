/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import java.util.Map;
import java.util.UUID;
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
public final class Attributes {
    /**
    /**
     * Defines an attribute value that denotes a missing value.
     */
    public static final long NULL_ATTRIBUTE_VALUE = Long.MIN_VALUE; //This is the same as WireCommands.NULL_ATTRIBUTE_VALUE

    /**
     * Prefix (Most Significant Bits) of the Id of all Core Attributes.
     */
    private static final long CORE_ATTRIBUTE_ID_PREFIX = Long.MIN_VALUE;

    /**
     * Defines an attribute that can be used to denote Segment creation time.
     */
    public static final UUID CREATION_TIME = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 0);

    /**
     * Defines an attribute that can be used to keep track of the number of events in a Segment.
     */
    public static final UUID EVENT_COUNT = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 1);

    /**
     * Defines an attribute that is used to keep scale policy type for stream segment.
     */
    public static final UUID SCALE_POLICY_TYPE = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 2);

    /**
     * Defines an attribute that is used to keep scale policy rate for stream segment.
     */
    public static final UUID SCALE_POLICY_RATE = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 3);

    /**
     * Defines an attribute that is used to set the value after which a Segment needs to be rolled over in Storage.
     */
    public static final UUID ROLLOVER_SIZE = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 4);

    /**
     * [Retired August 2018. Do not reuse as obsolete values may still linger around.]
     * Attribute Snapshot Location.
     */
    private static final UUID RETIRED_1 = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 5);

    /**
     * [Retired August 2018. Do not reuse as obsolete values may still linger around.]
     * Attribute Snapshot Length.
     */
    private static final UUID RETIRED_2 = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 6);

    /**
     * Defines an attribute that is used to store the next TableNodeId.
     */
    public static final UUID TABLE_NODE_ID = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 7);

    /**
     * Defines an attribute that is used to store the first index of a (Table) Segment that has not yet been indexed.
     */
    public static final UUID TABLE_INDEX_OFFSET = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 8);

    /**
     * Determines whether the given Attribute Id refers to a Core Attribute.
     *
     * @param attributeId The Attribute Id to check.
     * @return True if Core Attribute, false otherwise.
     */
    public static boolean isCoreAttribute(UUID attributeId) {
        return attributeId.getMostSignificantBits() == CORE_ATTRIBUTE_ID_PREFIX;
    }

    /**
     * Returns a new Map of Attribute Ids to Values containing only those Core Attributes from the given Map that do not
     * have a null value.
     *
     * @param attributes The Map of Attribute Ids to Values to filter from.
     * @return A new Map containing only Core Attribute Ids and Values (from the original map).
     */
    public static Map<UUID, Long> getCoreNonNullAttributes(Map<UUID, Long> attributes) {
        return attributes.entrySet().stream()
                         .filter(e -> Attributes.isCoreAttribute(e.getKey()) && e.getValue() != NULL_ATTRIBUTE_VALUE)
                         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
