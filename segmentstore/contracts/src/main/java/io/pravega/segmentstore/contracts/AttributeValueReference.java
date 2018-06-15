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

/**
 * Represents a Value for an Attribute that can be inferred based on the value of some other element.
 */
public abstract class AttributeValueReference {
    private static final Function<Long, Long> IDENTITY = v -> v;
    /**
     * A Function that, given the reference value, returns the desired value to be set for this Attribute.
     */
    @Getter
    private final Function<Long, Long> transformation;

    private AttributeValueReference(@Nonnull Function<Long, Long> transformation) {
        this.transformation = Preconditions.checkNotNull(transformation, "transformation");
    }

    /**
     * Represents a Value for an Attribute that will be evaluated based on the current Length of the Segment it is applied to.
     */
    public static class SegmentLength extends AttributeValueReference {
        /**
         * Creates a new instance of the SegmentLength class, with no transformation.
         */
        public SegmentLength() {
            super(IDENTITY);
        }

        /**
         * Creates a new instance of the SegmentLength class.
         *
         * @param transformation A Function that, given the reference value, returns the desired value to be set for this Attribute.
         */
        public SegmentLength(@Nonnull Function<Long, Long> transformation) {
            super(transformation);
        }
    }

    /**
     * Represents a Value for an Attribute that will be evaluated based on the value of another Attribute.
     */
    public static class Attribute extends AttributeValueReference {
        /**
         * The Attribute Id to fetch the value of.
         */
        @Getter
        private final UUID attributeId;

        /**
         * Creates a new instance of the AttributeValueReference.Attribute class, with no value transformation.
         *
         * @param attributeId The Attribute Id to fetch the value of.
         */
        public Attribute(@Nonnull UUID attributeId) {
            this(attributeId, IDENTITY);
        }

        /**
         * Creates a new instance of the AttributeValueReference.Attribute class.
         *
         * @param attributeId    The Attribute Id to fetch the value of.
         * @param transformation A Function that, given the reference value, returns the desired value to be set for this Attribute.
         */
        public Attribute(@Nonnull UUID attributeId, @Nonnull Function<Long, Long> transformation) {
            super(transformation);
            this.attributeId = Preconditions.checkNotNull(attributeId, "attributeId");
        }
    }
}
