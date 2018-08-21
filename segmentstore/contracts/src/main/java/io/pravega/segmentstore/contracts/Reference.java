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
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Represents a Reference that can be used to evaluate a value, as a function of another element's value.
 *
 * @param <T> Return type.
 */
@RequiredArgsConstructor
public abstract class Reference<T> {
    /**
     * A Function that transforms the value of an Attribute into another value.
     */
    @Getter
    private final Function<Long, T> transformation;

    /**
     * A Reference to the current Segment's length.
     *
     * @param <T>
     */
    public static class SegmentLength<T> extends Reference<T> {
        /**
         * Creates a new instance of the Reference.SegmentLength class.
         *
         * @param transformation A Function that, given the reference value, returns the desired value to be set for this Attribute.
         */
        public SegmentLength(@NonNull Function<Long, T> transformation) {
            super(transformation);
        }

        @Override
        public String toString() {
            return "SegmentLength" + (super.getTransformation() == null ? "" : "+Transformation");
        }
    }

    /**
     * A Reference to the current value of an Attribute on the current Segment.
     */
    public static class Attribute<T> extends Reference<T> {
        /**
         * The Attribute Id to fetch the value of.
         */
        @Getter
        private final UUID attributeId;

        /**
         * Creates a new instance of the Reference.Attribute class.
         *
         * @param attributeId    The Attribute Id to fetch the value of.
         * @param transformation A Function that, given the reference value, returns the desired value to be set for this Attribute.
         */
        public Attribute(@NonNull UUID attributeId, Function<Long, T> transformation) {
            super(transformation);
            this.attributeId = attributeId;
        }

        @Override
        public String toString() {
            return "Attribute[" + this.attributeId + "]" + (super.getTransformation() == null ? "" : "+Transformation");
        }
    }
}