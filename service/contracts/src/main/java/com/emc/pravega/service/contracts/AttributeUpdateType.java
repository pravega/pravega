/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.service.contracts;

import com.emc.pravega.common.util.EnumHelpers;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Defines a type of update for a particular Attribute.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum AttributeUpdateType {
    /**
     * No updates allowed: attribute value is fixed once set.
     */
    None((byte) 0),

    /**
     * Any updates will replace the current attribute value.
     */
    Replace((byte) 1),

    /**
     * Any updates will replace the current attribute value, but only if the new value is greater than the current
     * value (or no value defined currently). This does not require the updates to be consecutive. For example,
     * if A and B (A &lt; B) are updated concurrently, odds are that B will make it but A won't - this will be observed
     * by either A failing or both succeeding, but in the end, the final result will contain B.
     */
    ReplaceIfGreater((byte) 2),

    /**
     * Accumulates the new value to the existing attribute value (i.e., adds two numbers).
     */
    Accumulate((byte) 3);

    private static final AttributeUpdateType[] MAPPING = EnumHelpers.indexById(AttributeUpdateType.class, AttributeUpdateType::getTypeId);
    @Getter
    private final byte typeId;

    /**
     * Gets the AttributeUpdateType that has the given type id.
     *
     * @param typeId The type id to search by.
     * @return The mapped AttributeUpdateType, or null
     */
    public static AttributeUpdateType get(byte typeId) {
        if (typeId < 0 || typeId >= MAPPING.length || MAPPING[typeId] == null) {
            throw new IllegalArgumentException("Unsupported AttributeUpdateType Id " + typeId);
        }

        return MAPPING[typeId];
    }
}