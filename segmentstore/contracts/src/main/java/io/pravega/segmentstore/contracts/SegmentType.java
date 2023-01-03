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
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * Defines Segment Types. A Segment Type is a combination of Formats (how data are stored internally) and Roles (how is
 * the Segment used and how does the Segment Store rely on it).
 */
@RequiredArgsConstructor
public class SegmentType {
    //region Base Types
    /**
     * General Stream Segment with no special roles.
     */
    public static final SegmentType STREAM_SEGMENT = SegmentType.builder().build();
    /**
     * General Table Segment with no special roles.
     */
    public static final SegmentType TABLE_SEGMENT_HASH = SegmentType.builder().tableSegment().build();
    /**
     * General Transient Segment with no special roles.
     */
    public static final SegmentType TRANSIENT_SEGMENT = SegmentType.builder().transientSegment().build();

    //endregion

    //region Flags

    /*
     * Note to developers: DO NOT CHANGE the number (bit) representations of the fields below. They are used for bitwise
     * concatenation and their values are stored as Segment Attributes. Changing them would break backwards compatibility.
     * Adding new values is OK. Do not reuse retired values. Carefully consider the addition of new values as there are
     * a maximum of 64 flags that can be set using this scheme.
     */
    @VisibleForTesting
    static final long FORMAT_BASIC = 0b0000_0000L;
    @VisibleForTesting
    static final long FORMAT_TABLE_SEGMENT = 0b0000_0001L;
    @VisibleForTesting
    static final long FORMAT_FIXED_KEY_LENGTH_TABLE_SEGMENT = 0b0000_0100L | FORMAT_TABLE_SEGMENT;
    @VisibleForTesting
    static final long ROLE_INTERNAL = 0b0001_0000L;
    @VisibleForTesting
    static final long ROLE_SYSTEM = 0b0010_0000L | ROLE_INTERNAL;
    @VisibleForTesting
    static final long ROLE_CRITICAL = 0b0100_0000L;
    @VisibleForTesting
    static final long ROLE_TRANSIENT = 0b1000_0000L;
    private final long flags;

    //endregion

    //region Properties

    @Override
    public int hashCode() {
        return Long.hashCode(this.flags);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SegmentType) {
            return this.flags == ((SegmentType) obj).flags;
        }

        return false;
    }

    /**
     * Gets a numeric value that encodes the Segment Type.
     *
     * @return The value.
     */
    public long getValue() {
        return this.flags;
    }

    /**
     * Whether this {@link SegmentType} refers to a Table Segment (Sorted or not). If not a Table Segment, then it is
     * generally accepted that the Segment is a "Basic" Segment with no structure that the Segment Store knows or cares about.
     *
     * @return True if Table Segment, false otherwise.
     */
    public boolean isTableSegment() {
        return (this.flags & FORMAT_TABLE_SEGMENT) == FORMAT_TABLE_SEGMENT;
    }

    /**
     * Whether this {@link SegmentType} refers to a Fixed-Key-Length Table Segment (which implies {@link #isTableSegment()}.
     *
     * @return True if Fixed-Key-Length Table Segment, false otherwise.
     */
    public boolean isFixedKeyLengthTableSegment() {
        return (this.flags & FORMAT_FIXED_KEY_LENGTH_TABLE_SEGMENT) == FORMAT_FIXED_KEY_LENGTH_TABLE_SEGMENT;
    }

    /**
     * Whether this {@link SegmentType} refers to a Transient Segment.
     *
     * @return True if Transient Segment, false otherwise.
     */
    public boolean isTransientSegment() {
        return (this.flags & ROLE_TRANSIENT) == ROLE_TRANSIENT;
    }

    /**
     * Whether this {@link SegmentType} refers to a Segment (regardless of Format) that is for exclusive internal access.
     * If so, external requests may be denied on such Segments.
     *
     * @return True if internal, false otherwise.
     */
    public boolean isInternal() {
        return (this.flags & ROLE_INTERNAL) == ROLE_INTERNAL;
    }

    /**
     * Whether this {@link SegmentType} refers to a Segment (regardless of Format) that is required for the functioning
     * of the Segment Store. If so, this implies this Segment is also an {@link #isInternal()} Segment (not for external
     * access).
     *
     * @return True if a System Segment, false otherwise.
     */
    public boolean isSystem() {
        return (this.flags & ROLE_SYSTEM) == ROLE_SYSTEM;
    }

    /**
     * Whether this {@link SegmentType} refers to a Segment (regardless of Format or other Roles) that is ESSENTIAL to
     * the good functioning of the Segment Store. Such Segments require the highest priority in resource utilization or
     * speed of execution in order; if that cannot be guaranteed, the Segment Store may suffer a performance degradation
     * as a result.
     *
     * @return True if a Critical Segment, false otherwise.
     */
    public boolean isCritical() {
        return (this.flags & ROLE_CRITICAL) == ROLE_CRITICAL;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(String.format("[%s]: Base", this.flags));
        if (isFixedKeyLengthTableSegment()) {
            result.append(", Table Segment (Fixed-Key-Length)");
        } else if (isTableSegment()) {
            result.append(", Table Segment");
        }

        if (isTransientSegment()) {
            result.append(", Transient");
        }

        if (isSystem()) {
            result.append(", System");
        }

        if (isCritical()) {
            result.append(", Critical");
        }

        if (isInternal()) {
            result.append(", Internal");
        }

        return result.toString();
    }

    //endregion

    //region Segment Attribute Integration

    /**
     * Creates a new {@link SegmentType} instance from the given {@link Map} that contains a Segment's Attributes.
     *
     * Attributes Checked:
     * - {@link Attributes#ATTRIBUTE_SEGMENT_TYPE} (base value)
     * - {@link TableAttributes#INDEX_OFFSET} (whether a Table Segment - if not already in base value)
     *
     * The {@link TableAttributes} is necessary to support upgrades. {@link SegmentType} was introduced in Pravega 0.9,
     * however Table Segments were introduced in prior versions. Fixed-Key-Length Table Segments were introduced post 0.9,
     * so they should already have the correct Segment Type set.
     *
     * @param segmentAttributes A {@link Map} containing the Segment's Attributes to load from.
     * @return A {@link SegmentType}.
     */
    public static SegmentType fromAttributes(Map<AttributeId, Long> segmentAttributes) {
        long type = segmentAttributes.getOrDefault(Attributes.ATTRIBUTE_SEGMENT_TYPE, FORMAT_BASIC);
        Builder builder = new Builder(type);
        if (segmentAttributes.containsKey(TableAttributes.INDEX_OFFSET)) {
            builder.tableSegment();
        }

        return builder.build();
    }

    /**
     * Stores the {@link #getValue()} into an {@link Attributes#ATTRIBUTE_SEGMENT_TYPE} entry in the given map. No other
     * entries in the given map are altered.
     *
     * @param segmentAttributes A {@link Map} representing the segment attributes to update.
     * @return True if the value was inserted or modified, false if an identical value already existed.
     */
    public boolean intoAttributes(Map<AttributeId, Long> segmentAttributes) {
        Long previous = segmentAttributes.put(Attributes.ATTRIBUTE_SEGMENT_TYPE, this.flags);
        return previous == null || previous != this.flags;
    }

    //endregion

    //region Builder

    /**
     * Creates a new {@link Builder} with an initial Basic Format.
     *
     * @return A new {@link Builder} instance.
     */
    public static Builder builder() {
        return new Builder(FORMAT_BASIC);
    }

    /**
     * Creates a new {@link Builder} that inherits from an existing {@link SegmentType} instance.
     *
     * @param base The {@link SegmentType} to inherit from.
     * @return A new {@link Builder} instance.
     */
    public static Builder builder(SegmentType base) {
        return new Builder(base.flags);
    }

    /**
     * Builder for {@link SegmentType}.
     */
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private long flags;

        public Builder tableSegment() {
            this.flags |= FORMAT_TABLE_SEGMENT;
            return this;
        }

        public Builder fixedKeyLengthTableSegment() {
            this.flags |= FORMAT_FIXED_KEY_LENGTH_TABLE_SEGMENT;
            return this;
        }

        public Builder transientSegment() {
            this.flags |= ROLE_TRANSIENT;
            return this;
        }

        public Builder system() {
            this.flags |= ROLE_SYSTEM;
            return this;
        }

        public Builder critical() {
            this.flags |= ROLE_CRITICAL;
            return this;
        }

        public Builder internal() {
            this.flags |= ROLE_INTERNAL;
            return this;
        }

        public SegmentType build() {
            return new SegmentType(this.flags);
        }
    }

    //endregion
}
