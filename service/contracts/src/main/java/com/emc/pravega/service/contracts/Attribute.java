package com.emc.pravega.service.contracts;

import com.google.common.base.Preconditions;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Getter;

/**
 * Represents a Segment Attribute.
 */
@Getter
@EqualsAndHashCode(of = {"id"})
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Attribute {
    //region Well Known Attributes
    /**
     * Prefix (Most Significant Bits) of the Id of all well-known attributes.
     */
    public static final long WELL_KNOWN_ID_PREFIX = Long.MIN_VALUE;

    /**
     * Defines an attribute that can be used to denote Segment creation time.
     */
    public static final Attribute CREATION_TIME = new Attribute(new UUID(WELL_KNOWN_ID_PREFIX, 0), Attribute.UpdateType.None);

    /**
     * Defines an attribute that can be used to keep track of the number of events in a Segment.
     */
    public static final Attribute EVENT_COUNT = new Attribute(new UUID(WELL_KNOWN_ID_PREFIX, 0), Attribute.UpdateType.Accumulate);

    //endregion

    //region Members

    private final UUID id;
    private final UpdateType updateType;

    @Override
    public String toString() {
        return String.format("Id = %s, UpdateType = %s", this.id, this.updateType);
    }

    //endregion

    /**
     * Creates a new Dynamic Attribute. A Dynamic Attribute is any attribute that does not have a well-known Id (such as
     * the ones defined as static in this class). They are usually generated and used externally to the StreamSegmentStore.
     *
     * @param attributeId The Id of the attribute.
     * @param updateType  The Update Type of the attribute.
     * @return A new instance of the Attribute class.
     * @throws IllegalArgumentException If attributeId.getMostSignificantBits() matches the WELL_KNOWN_ID_PREFIX.
     */
    public static Attribute dynamic(UUID attributeId, Attribute.UpdateType updateType) {
        Preconditions.checkArgument(attributeId.getMostSignificantBits() != WELL_KNOWN_ID_PREFIX,
                "Cannot create a dynamic Attribute with a Well-Known Id Prefix.");
        return new Attribute(attributeId, updateType);
    }

    //region UpdateType

    /**
     * Defines a type of update for a particular Attribute.
     */
    public enum UpdateType {
        /**
         * No updates allowed: attribute value is fixed once set.
         */
        None,

        /**
         * Any updates will replace the current attribute value.
         */
        Replace,

        /**
         * Any updates will replace the current attribute value, but only if the new value is greater than the current
         * value (or no value defined currently).
         */
        ReplaceIfGreater,

        /**
         * Accumulates the new value to the existing attribute value (i.e., adds two numbers).
         */
        Accumulate
    }

    //endregion
}
