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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Defines an Attribute Id for Segments.
 */
public abstract class AttributeId implements Comparable<AttributeId> {
    /**
     * The maximum length for any Attribute Id. Regardless of implementation, Attribute Id serialization may not exceed
     * this value.
     */
    public static final int MAX_LENGTH = 256;

    /**
     * Creates a new {@link AttributeId.UUID} using the given bits.
     *
     * @param mostSignificantBits  The UUID's MSB.
     * @param leastSignificantBits The UUID's LSB.
     * @return A new instance of {@link AttributeId.UUID}.
     */
    public static AttributeId uuid(long mostSignificantBits, long leastSignificantBits) {
        return new UUID(mostSignificantBits, leastSignificantBits);
    }

    /**
     * Creates a new {@link AttributeId.UUID} using the given {@link UUID}.
     *
     * @param uuid The {@link java.util.UUID} to use.
     * @return A new instance of {@link AttributeId.UUID}.
     */
    public static AttributeId fromUUID(java.util.UUID uuid) {
        return new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * Creates a new {@link AttributeId.Variable} using the given byte array.
     *
     * @param data The byte array to wrap. NOTE: this will not be duplicated. Any changes to the underlying buffer will
     *             be reflected in this Attribute Id.
     * @return A new instance of {@link AttributeId.Variable} wrapping the given byte array.
     */
    public static AttributeId from(byte[] data) {
        return new Variable(data);
    }

    /**
     * Creates a new {@link AttributeId.UUID} with random content.
     *
     * @return A new instance of {@link AttributeId.UUID} that is generated using {@link java.util.UUID#randomUUID()}.
     */
    @VisibleForTesting
    public static AttributeId randomUUID() {
        return fromUUID(java.util.UUID.randomUUID());
    }

    /**
     * Creates a new {@link AttributeId.Variable} with random content.
     *
     * @param length The length of the {@link AttributeId}, in bytes.
     * @return A new instance of {@link AttributeId.Variable} with random content.
     */
    @VisibleForTesting
    public static AttributeId random(int length) {
        Preconditions.checkArgument(length > 0 && length <= MAX_LENGTH, "length must be a positive number less than or equal to %s.", MAX_LENGTH);
        byte[] result = new byte[length];
        RandomFactory.create().nextBytes(result);
        return AttributeId.from(result);
    }

    /**
     * Gets a value indicating whether this {@link AttributeId} instance is a UUID Attribute.
     *
     * @return True if this instance is an {@link AttributeId.UUID}, false otherwise.
     */
    public abstract boolean isUUID();

    /**
     * Gets a value indicating the size, in bytes, of this {@link AttributeId}.
     *
     * @return The size.
     */
    public abstract int byteCount();

    /**
     * Gets a 64-bit value representing the Bits beginning at the given index.
     *
     * @param position The 0-based position representing the bit group to get. Each position maps to an 8-byte (64-bit)
     *                 block of data (position 0 maps to index 0, position 1 maps to index 8, ..., position i maps to index 8*i).
     * @return The value of the bit group.
     */
    public abstract long getBitGroup(int position);

    /**
     * Serializes this {@link AttributeId} into a {@link ByteArraySegment}. This should be appropriate for most cases
     * where it will be later needed to deserialize back into an {@link AttributeId}. For {@link AttributeId.UUID} instances,
     * this will use signed Long serialization, which may not be appropriate for comparison using {@link BufferViewComparator}.
     *
     * @return A {@link ByteArraySegment} of length {@link #byteCount()} containing a serialization of this {@link AttributeId}.
     */
    public abstract ByteArraySegment toBuffer();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    @Override
    public abstract int compareTo(AttributeId other);

    //region UUID

    /**
     * A 16-byte {@link AttributeId} that maps to a {@link UUID}. These should be the default Attribute Id type for
     * segments, except if a different Attribute Id length is requested (i.e., using {@link Attributes#ATTRIBUTE_ID_LENGTH}).
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class UUID extends AttributeId {
        public static final int ATTRIBUTE_ID_LENGTH = 2 * Long.BYTES;
        private final long mostSignificantBits;
        private final long leastSignificantBits;

        @Override
        public boolean isUUID() {
            return true;
        }

        @Override
        public int byteCount() {
            return ATTRIBUTE_ID_LENGTH;
        }

        @Override
        public long getBitGroup(int position) {
            switch (position) {
                case 0:
                    return this.mostSignificantBits;
                case 1:
                    return this.leastSignificantBits;
                default:
                    throw new IllegalArgumentException(this.getClass().getName() + " only supports bit groups 0 and 1. Requested: " + position);
            }
        }

        /**
         * Gets a {@link java.util.UUID} representation of this {@link AttributeId}.
         *
         * @return A new {@link java.util.UUID}.
         * @throws UnsupportedOperationException If {@link #isUUID()} is false.
         */
        public java.util.UUID toUUID() {
            return new java.util.UUID(this.mostSignificantBits, this.leastSignificantBits);
        }

        @Override
        public ByteArraySegment toBuffer() {
            ByteArraySegment result = new ByteArraySegment(new byte[ATTRIBUTE_ID_LENGTH]);
            result.setLong(0, this.mostSignificantBits);
            result.setLong(Long.BYTES, this.leastSignificantBits);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof UUID) {
                UUID id = (UUID) obj;
                return this.mostSignificantBits == id.mostSignificantBits && this.leastSignificantBits == id.leastSignificantBits;
            }

            return false;
        }

        @Override
        public int hashCode() {
            long hash = this.mostSignificantBits ^ this.leastSignificantBits;
            return (int) (hash >> 32) ^ (int) hash;
        }

        @Override
        public int compareTo(AttributeId val) {
            UUID uuid = (UUID) val; // This will throw an exception if we try to compare the wrong types - it's OK.
            int r = Long.compare(this.mostSignificantBits, uuid.mostSignificantBits);
            if (r == 0) {
                r = Long.compare(this.leastSignificantBits, uuid.leastSignificantBits);
            }
            return r;
        }

        @Override
        public String toString() {
            return toUUID().toString();
        }
    }

    //endregion

    //region Variable

    /**
     * An {@link AttributeId} that can encode any-length attribute ids (up to {@link #MAX_LENGTH}).
     */
    public static final class Variable extends AttributeId {
        private static final BufferViewComparator COMPARATOR = BufferViewComparator.create();
        private final byte[] data;
        private final int hashCode;

        private Variable(byte[] data) {
            this.data = data;
            this.hashCode = (int) BufferView.wrap(this.data).hash();
        }

        @Override
        public boolean isUUID() {
            return false;
        }

        @Override
        public int byteCount() {
            return this.data.length;
        }

        @Override
        public long getBitGroup(int position) {
            return BitConverter.readLong(this.data, position << 3); // This will do necessary checks for us.
        }

        @Override
        public ByteArraySegment toBuffer() {
            return new ByteArraySegment(this.data);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Variable) {
                return Arrays.equals(this.data, ((Variable) obj).data);
            }

            return false;
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }

        @Override
        public int compareTo(AttributeId val) {
            // This will throw an exception if we try to compare the wrong types - it's OK.
            return COMPARATOR.compare(this.data, ((Variable) val).data);
        }

        @Override
        public String toString() {
            return String.format("Length = %s, Hashcode = %s", this.data.length, this.hashCode);
        }

        /**
         * Gets an {@link AttributeId.Variable} of the given length ({@link #byteCount()} will equal that) that is the
         * minimum possible value for that length.
         *
         * @param length The length.
         * @return The minimum possible {@link AttributeId.Variable} for the given length.
         */
        public static AttributeId minValue(int length) {
            return new AttributeId.Variable(BufferViewComparator.getMinValue(length));
        }

        /**
         * Gets an {@link AttributeId.Variable} of the given length ({@link #byteCount()} will equal that) that is the
         * maximum possible value for that length.
         *
         * @param length The length.
         * @return The maximum possible {@link AttributeId.Variable} for the given length.
         */
        public static AttributeId maxValue(int length) {
            return new AttributeId.Variable(BufferViewComparator.getMaxValue(length));
        }
    }

    //endregion
}
