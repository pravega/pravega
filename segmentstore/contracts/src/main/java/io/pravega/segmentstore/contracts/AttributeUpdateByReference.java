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
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;

/**
 * An AttributeUpdate that will set the value of an Attribute based on an evaluated function.
 */
@NotThreadSafe
public class AttributeUpdateByReference extends AttributeUpdate {
    private boolean valueSet; // This is needed since Value is non-nullable.
    /**
     * The Reference to evaluate in order to determine AttributeId.
     */
    @Getter
    private final Reference<UUID> idReference;

    /**
     * The Reference to evaluate in order to determine Attribute value.
     */
    @Getter
    private final Reference<Long> valueReference;

    /**
     * Creates a new instance of the AttributeUpdateByReference class, except for ReplaceIfEquals.
     *
     * @param attributeId    The AttributeId to update.
     * @param updateType     The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param valueReference The Reference to evaluate.
     */
    public AttributeUpdateByReference(@Nonnull UUID attributeId, @Nonnull AttributeUpdateType updateType, @Nonnull Reference<Long> valueReference) {
        super(attributeId, updateType, Attributes.NULL_ATTRIBUTE_VALUE);
        this.idReference = null;
        this.valueReference = Preconditions.checkNotNull(valueReference, "valueReference");
    }

    /**
     * Creates a new instance of the AttributeUpdateByReference class, except for ReplaceIfEquals.
     *
     * @param idReference    The Reference to evaluate in order to determine AttributeId.
     * @param updateType     The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param valueReference The Reference to evaluate in order to determine Attribute Value.
     */
    public AttributeUpdateByReference(@Nonnull Reference<UUID> idReference, @Nonnull AttributeUpdateType updateType, @Nonnull Reference<Long> valueReference) {
        super(null, updateType, Attributes.NULL_ATTRIBUTE_VALUE);
        this.idReference = Preconditions.checkNotNull(idReference, "idReference");
        this.valueReference = Preconditions.checkNotNull(valueReference, "valueReference");
    }

    /**
     * Creates a new instance of the AttributeUpdateByReference class.
     *
     * @param attributeId     The AttributeId to update.
     * @param updateType      The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param valueReference  The AttributeValueReference to evaluate.
     * @param comparisonValue The value to compare against.
     */
    public AttributeUpdateByReference(@Nonnull UUID attributeId, @Nonnull AttributeUpdateType updateType, @Nonnull Reference<Long> valueReference, long comparisonValue) {
        super(attributeId, updateType, Attributes.NULL_ATTRIBUTE_VALUE, comparisonValue);
        this.idReference = null;
        this.valueReference = Preconditions.checkNotNull(valueReference, "valueReference");
    }

    /**
     * Creates a new instance of the AttributeUpdateByReference class.
     *
     * @param idReference     The Reference to evaluate in order to determine AttributeId.
     * @param updateType      The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param valueReference The Reference to evaluate in order to determine Attribute Value.
     * @param comparisonValue The value to compare against.
     */
    public AttributeUpdateByReference(@Nonnull Reference<UUID> idReference, @Nonnull AttributeUpdateType updateType, @Nonnull Reference<Long> valueReference, long comparisonValue) {
        super(null, updateType, Attributes.NULL_ATTRIBUTE_VALUE, comparisonValue);
        this.idReference = Preconditions.checkNotNull(idReference, "idReference");
        this.valueReference = Preconditions.checkNotNull(valueReference, "valueReference");
    }

    @Override
    public UUID getAttributeId() {
        UUID id = super.getAttributeId();
        Preconditions.checkState(id != null, "id not set");
        return id;
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

