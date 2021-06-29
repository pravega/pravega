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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Defines an Attribute Value that is evaluated dynamically, usually as a function of another Segment property.
 */
public abstract class DynamicAttributeValue {

    /**
     * Evaluates this {@link DynamicAttributeValue} against the given {@link SegmentProperties}.
     *
     * @param segmentProperties The {@link SegmentProperties} to evaluate against.
     * @return The evaluated Attribute Value.
     */
    public abstract long evaluate(SegmentProperties segmentProperties);

    /**
     * Creates a new {@link DynamicAttributeValue} that evaluates to a {@link SegmentProperties#getLength()}.
     *
     * @param adjustment The adjustment to apply to {@link SegmentProperties#getLength()}. For example, if the Segment
     *                   Length is 100, and {@code adjustment} is -5, then this will evaluate to 100 - 5 = 95.
     * @return A new {@link DynamicAttributeValue}.
     */
    public static DynamicAttributeValue segmentLength(long adjustment) {
        return new SegmentLength(adjustment);
    }

    /**
     * Represents a {@link DynamicAttributeValue} that evaluates to the current length of the Segment, with an optional
     * adjustment.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class SegmentLength extends DynamicAttributeValue {
        /**
         * The adjustment to apply to the Segment's Length.
         */
        private final long adjustment;

        @Override
        public String toString() {
            return String.format("Adjustment = %s", this.adjustment);
        }

        @Override
        public long evaluate(SegmentProperties segmentProperties) {
            return segmentProperties.getLength() + this.adjustment;
        }
    }
}
