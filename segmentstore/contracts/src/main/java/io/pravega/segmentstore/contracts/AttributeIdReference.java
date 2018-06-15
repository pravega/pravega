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
 * Represents an Id for an Attribute that can be inferred based on the value of another Attribute.
 */
public class AttributeIdReference {
    /**
     * The Attribute Id to base the new value off.
     */
    @Getter
    private final UUID referenceAttributeId;
    /**
     * A Function that, given the reference value of another Attribute, returns the desired Attribute Id.
     */
    @Getter
    private final Function<Long, UUID> transformation;

    /**
     * Creates a new instance of the AttributeIdReference class.
     *
     * @param referenceAttributeId The Attribute Id to base the new value off.
     * @param transformation       A Function that, given the reference value of another Attribute, returns the desired
     *                             Attribute Id.
     */
    public AttributeIdReference(@Nonnull UUID referenceAttributeId, @Nonnull Function<Long, UUID> transformation) {
        this.referenceAttributeId = Preconditions.checkNotNull(referenceAttributeId, "referenceAttributeId");
        this.transformation = Preconditions.checkNotNull(transformation, "transformation");
    }
}
