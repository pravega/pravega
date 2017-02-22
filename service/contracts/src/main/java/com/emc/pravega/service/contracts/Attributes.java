/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.service.contracts;

import java.util.UUID;

/**
 * Defines a set of well known attributes.
 */
public final class Attributes {
    /**
     * Prefix (Most Significant Bits) of the Id of all well-known attributes.
     */
    public static final long WELL_KNOWN_ID_PREFIX = Long.MIN_VALUE;

    /**
     * Defines an attribute that can be used to denote Segment creation time.
     */
    public static final UUID CREATION_TIME = new UUID(WELL_KNOWN_ID_PREFIX, 0);

    /**
     * Defines an attribute that can be used to keep track of the number of events in a Segment.
     */
    public static final UUID EVENT_COUNT = new UUID(WELL_KNOWN_ID_PREFIX, 1);

    /**
     * Determines whether the given Attribute Id refers to a dynamic attribute (vs a well-known one).
     *
     * @param attributeId The Attribute Id to check.
     * @return True if dynamic, false otherwise.
     */
    public static boolean isDynamic(UUID attributeId) {
        return attributeId.getMostSignificantBits() != WELL_KNOWN_ID_PREFIX;
    }
}
