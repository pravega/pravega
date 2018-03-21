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

import java.util.UUID;

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
     * Defines an attribute that is used to store the offset within the Attribute sub-segment where the last Snapshot begins.
     */
    public static final UUID LAST_ATTRIBUTE_SNAPSHOT_OFFSET = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 5);

    /**
     * Defines an attribute that is used to store the length (in bytes) of the last Attribute Snapshot.
     */
    public static final UUID LAST_ATTRIBUTE_SNAPSHOT_LENGTH = new UUID(CORE_ATTRIBUTE_ID_PREFIX, 6);

    /**
     * Determines whether the given Attribute Id refers to a Core Attribute.
     *
     * @param attributeId The Attribute Id to check.
     * @return True if Core Attribute, false otherwise.
     */
    public static boolean isCoreAttribute(UUID attributeId){
        return attributeId.getMostSignificantBits() == CORE_ATTRIBUTE_ID_PREFIX;
    }
}
