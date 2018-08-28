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
import lombok.NonNull;

/**
 * An AttributeUpdate that will set the value of an Attribute based on an evaluated function.
 */
@NotThreadSafe
public class AttributeUpdateByReference extends AttributeUpdate {
    private boolean valueSet; // This is needed since Value is non-nullable.
    /**
     * The AttributeReference to evaluate in order to determine AttributeId.
     */
    @Getter
    private final AttributeReference<UUID> idReference;

    /**
     * The AttributeReference to evaluate in order to determine Attribute value.
     */
    @Getter
    private final AttributeReference<Long> valueReference;

    /**
     * Creates a new instance of the AttributeUpdateByReference class, except for ReplaceIfEquals.
     *
     * @param attributeId    The AttributeId to update.
     * @param updateType     The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param valueReference The AttributeReference to evaluate.
     */
    public AttributeUpdateByReference(@NonNull UUID attributeId, @NonNull AttributeUpdateType updateType, @NonNull AttributeReference<Long> valueReference) {
        super(attributeId, updateType, Attributes.NULL_ATTRIBUTE_VALUE);
        this.idReference = null;
        this.valueReference = valueReference;
    }

    /**
     * Creates a new instance of the AttributeUpdateByReference class, except for ReplaceIfEquals.
     *
     * @param idReference    The AttributeReference to evaluate in order to determine AttributeId.
     * @param updateType     The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param valueReference The AttributeReference to evaluate in order to determine Attribute Value.
     */
    public AttributeUpdateByReference(@NonNull AttributeReference<UUID> idReference, @NonNull AttributeUpdateType updateType, @NonNull AttributeReference<Long> valueReference) {
        super(null, updateType, Attributes.NULL_ATTRIBUTE_VALUE);
        this.idReference = idReference;
        this.valueReference = valueReference;
    }

    /**
     * Creates a new instance of the AttributeUpdateByReference class, except for ReplaceIfEquals.
     *
     * @param idReference The AttributeReference to evaluate in order to determine AttributeId.
     * @param updateType  The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param value       The AttributeValue to set..
     */
    public AttributeUpdateByReference(@NonNull AttributeReference<UUID> idReference, @NonNull AttributeUpdateType updateType, long value) {
        super(null, updateType, value);
        this.idReference = idReference;
        this.valueReference = null;
        this.valueSet = true;
    }

    /**
     * Creates a new instance of the AttributeUpdateByReference class.
     *
     * @param attributeId     The AttributeId to update.
     * @param updateType      The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param valueReference  The AttributeValueReference to evaluate.
     * @param comparisonValue The value to compare against.
     */
    public AttributeUpdateByReference(@NonNull UUID attributeId, @NonNull AttributeUpdateType updateType, @NonNull AttributeReference<Long> valueReference, long comparisonValue) {
        super(attributeId, updateType, Attributes.NULL_ATTRIBUTE_VALUE, comparisonValue);
        this.idReference = null;
        this.valueReference = valueReference;
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

    @Override
    public String toString() {
        return String.format("AttributeId = %s, Value = %s, UpdateType = %s",
                super.getAttributeId() == null ? "(by-ref)" : super.getAttributeId(),
                this.valueSet ? super.getValue() : "(by-ref)",
                super.getUpdateType());
    }
}

