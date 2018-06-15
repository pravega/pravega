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

import com.google.common.base.Preconditions;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents a Reference that can be used to evaluate a value, as a function of another element's value.
 *
 * @param <T> Return type.
 */
@RequiredArgsConstructor
public abstract class Reference<T> {
    /**
     * A Function that, given the reference value of another Attribute, returns the desired Attribute Id.
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
         */
        public SegmentLength() {
            super(null);
        }

        /**
         * Creates a new instance of the Reference.SegmentLength class.
         *
         * @param transformation A Function that, given the reference value, returns the desired value to be set for this Attribute.
         */
        public SegmentLength(Function<Long, T> transformation) {
            super(transformation);
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
         * Creates a new instance of the Reference.Attribute class, with no value transformation.
         *
         * @param attributeId The Attribute Id to fetch the value of.
         */
        public Attribute(@Nonnull UUID attributeId) {
            this(attributeId, null);
        }

        /**
         * Creates a new instance of the Reference.Attribute class.
         *
         * @param attributeId    The Attribute Id to fetch the value of.
         * @param transformation A Function that, given the reference value, returns the desired value to be set for this Attribute.
         */
        public Attribute(@Nonnull UUID attributeId, Function<Long, T> transformation) {
            super(transformation);
            this.attributeId = Preconditions.checkNotNull(attributeId, "attributeId");
        }
    }
}