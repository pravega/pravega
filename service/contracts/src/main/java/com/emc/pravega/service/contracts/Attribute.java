package com.emc.pravega.service.contracts;

import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

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

    //region Serialization

    /**
     * Serializes this Attribute to the given DataOutputStream.
     *
     * @param target The DataOutputStream to serialize to.
     * @throws IOException If an exception occurred.
     */
    public void serialize(DataOutputStream target) throws IOException {
        target.writeLong(this.id.getMostSignificantBits());
        target.writeLong(this.id.getLeastSignificantBits());
        target.writeByte(this.updateType.getTypeId());
    }

    /**
     * Deserializes an Attribute from the given DataInputStream.
     *
     * @param source The DataInputStream to deserialize from.
     * @return The deserialized Attribute.
     * @throws IOException If an exception occurred.
     */
    public static Attribute deserialize(DataInputStream source) throws IOException {
        long idMostSig = source.readLong();
        long idLeastSig = source.readLong();
        UUID attributeId = new UUID(idMostSig, idLeastSig);
        UpdateType updateType = UpdateType.get(source.readByte());
        return new Attribute(attributeId, updateType);
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
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public enum UpdateType {
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
         * value (or no value defined currently).
         */
        ReplaceIfGreater((byte) 2),

        /**
         * Accumulates the new value to the existing attribute value (i.e., adds two numbers).
         */
        Accumulate((byte) 3);

        @Getter
        private final byte typeId;
        private static final UpdateType[] mapping;

        static {
            UpdateType[] values = UpdateType.values();
            mapping = new UpdateType[Arrays.stream(values).mapToInt(UpdateType::getTypeId).max().orElse(0) + 1];
            for (UpdateType ut : values) {
                mapping[ut.getTypeId()] = ut;
            }
        }

        /**
         * Gets the UpdateType that has the given type id.
         *
         * @param typeId The type id to search by.
         * @return The mapped UpdateType, or null
         */
        public static UpdateType get(byte typeId) {
            if (typeId < 0 || typeId >= mapping.length || mapping[typeId] == null) {
                throw new IllegalArgumentException("Unsupported UpdateType Id " + typeId);
            }

            return mapping[typeId];
        }
    }

    //endregion
}
