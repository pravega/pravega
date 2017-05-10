/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.contracts;

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
     * Defines an attribute that is used to keep scale policy type for stream segment.
     */
    public static final UUID SCALE_POLICY_TYPE = new UUID(WELL_KNOWN_ID_PREFIX, 2);

    /**
     * Defines an attribute that is used to keep scale policy rate for stream segment.
     */
    public static final UUID SCALE_POLICY_RATE = new UUID(WELL_KNOWN_ID_PREFIX, 3);

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
