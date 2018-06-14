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
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;

/**
 * An AttributeUpdate that will set the value of an Attribute based on an evaluated function.
 */
@NotThreadSafe
public class AttributeUpdateByReference extends AttributeUpdate {
    private boolean valueSet;
    /**
     * The AttributeValueReference to evaluate.
     */
    @Getter
    private final AttributeValueReference reference;

    /**
     * Creates a new instance of the AttributeUpdateByReference class, except for ReplaceIfEquals.
     *
     * @param attributeId A UUID representing the ID of the attribute to update.
     * @param updateType  The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param reference   The AttributeValueReference to evaluate.
     */
    public AttributeUpdateByReference(UUID attributeId, AttributeUpdateType updateType, AttributeValueReference reference) {
        super(attributeId, updateType, Attributes.NULL_ATTRIBUTE_VALUE);
        this.reference = Preconditions.checkNotNull(reference, "reference");
    }

    /**
     * Creates a new instance of the AttributeUpdateByReference class.
     *
     * @param attributeId     A UUID representing the ID of the attribute to update.
     * @param updateType      The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param reference       The AttributeValueReference to evaluate.
     * @param comparisonValue The value to compare against.
     */
    public AttributeUpdateByReference(UUID attributeId, AttributeUpdateType updateType, AttributeValueReference reference, long comparisonValue) {
        super(attributeId, updateType, Attributes.NULL_ATTRIBUTE_VALUE, comparisonValue);
        this.reference = Preconditions.checkNotNull(reference, "reference");
    }

    @Override
    public void setValue(long value) {
        this.valueSet = true;
        super.setValue(value);
    }

    @Override
    public long getValue() {
        Preconditions.checkState(this.valueSet, "value not set");
        return super.getValue();
    }

    @Override
    public boolean equals(Object obj) {
        return this.valueSet && super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}

