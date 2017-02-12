/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    public static final Attribute EVENT_COUNT = new Attribute(new UUID(WELL_KNOWN_ID_PREFIX, 1), Attribute.UpdateType.Accumulate);

    /**
     * Defines attributes that capture scaling policy.
     */
    public static final Attribute SCALE_POLICY_TYPE = new Attribute(new UUID(WELL_KNOWN_ID_PREFIX, 2), UpdateType.Replace);
    public static final Attribute SCALE_POLICY_RATE = new Attribute(new UUID(WELL_KNOWN_ID_PREFIX, 3), UpdateType.Replace);

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
        Preconditions.checkArgument(isDynamic(attributeId), "Cannot create a dynamic Attribute with a Well-Known Id Prefix.");
        return new Attribute(attributeId, updateType);
    }

    /**
     * Determines whether the given Attribute Id refers to a dynamic attribute (vs a well-known one).
     *
     * @param attributeId The Attribute Id to test.
     * @return True if dynamic, false otherwise.
     */
    public static boolean isDynamic(UUID attributeId) {
        return attributeId.getMostSignificantBits() != WELL_KNOWN_ID_PREFIX;
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

        private static final UpdateType[] MAPPING;
        @Getter
        private final byte typeId;

        static {
            UpdateType[] values = UpdateType.values();
            MAPPING = new UpdateType[Arrays.stream(values).mapToInt(UpdateType::getTypeId).max().orElse(0) + 1];
            for (UpdateType ut : values) {
                MAPPING[ut.getTypeId()] = ut;
            }
        }

        /**
         * Gets the UpdateType that has the given type id.
         *
         * @param typeId The type id to search by.
         * @return The mapped UpdateType, or null
         */
        public static UpdateType get(byte typeId) {
            if (typeId < 0 || typeId >= MAPPING.length || MAPPING[typeId] == null) {
                throw new IllegalArgumentException("Unsupported UpdateType Id " + typeId);
            }

            return MAPPING[typeId];
        }
    }

    //endregion
}
