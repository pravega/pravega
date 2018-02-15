/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.util.SequencedItemList;
import lombok.RequiredArgsConstructor;

/**
 * Base class for a Log Operation.
 */
public abstract class Operation implements SequencedItemList.Element {
    //region Members

    public static final long NO_SEQUENCE_NUMBER = Long.MIN_VALUE;
    private long sequenceNumber;

    //endregion

    //region Constructors

    /**
     * Creates a new instance of the Operation class.
     */
    public Operation() {
        this.sequenceNumber = NO_SEQUENCE_NUMBER;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the Sequence Number for this Operation.
     * The Operation Sequence Number is a unique, strictly monotonically increasing number that assigns order to operations.
     *
     * @return The Sequence Number for this Operation.
     */
    @Override
    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    /**
     * Sets the Sequence Number for this operation, if not already set.
     *
     * @param value The Sequence Number to set.
     * @throws IllegalStateException    If the Sequence Number has already been set.
     * @throws IllegalArgumentException If the Sequence Number is negative.
     */
    public void setSequenceNumber(long value) {
        Preconditions.checkState(this.sequenceNumber < 0, "Sequence Number has been previously set for this entry. Cannot set a new one.");
        Exceptions.checkArgument(value >= 0, "value", "Sequence Number must be a non-negative number.");

        this.sequenceNumber = value;
    }

    /**
     * Gets a value indicating whether this operation can be serialized to the DurableDataLog. This generally differentiates
     * between control operations (i.e. ProbeOperations) and operations that serve a real purpose.
     *
     * @return True if can (and must) serialize, false otherwise.
     */
    public boolean canSerialize() {
        return true;
    }

    @Override
    public String toString() {
        return String.format("%s: SequenceNumber = %d", this.getClass().getSimpleName(), getSequenceNumber());
    }

    protected String toString(Object value, Object notSetValue) {
        if (value == notSetValue) {
            return "<not set>";
        } else if (value == null) {
            return "<null>";
        } else {
            return value.toString();
        }
    }

    //endregion

    //region Serialization

    protected void ensureSerializationConditions() {
        ensureSerializationCondition(this.sequenceNumber >= 0, "Sequence Number has not been assigned.");
    }

    /**
     * If the given condition is false, throws an exception with the given message.
     *
     * @param isTrue  Whether the condition is true or false.
     * @param message The message to include in the exception.
     * @throws IllegalStateException The exception that is thrown.
     */
    void ensureSerializationCondition(boolean isTrue, String message) {
        Preconditions.checkState(isTrue, "Unable to serialize Operation: %s", message);
    }

    /**
     * ObjectBuilder that pre-instantiates the instance to use. This is so that we don't use a shadow object every time
     * we need to deserialize a new operation.
     * @param <T> Type of the object.
     */
    @RequiredArgsConstructor
    protected static class OperationBuilder<T extends Operation> implements ObjectBuilder<T> {
        protected final T instance;

        @Override
        public T build() {
            return this.instance;
        }
    }

    // endregion
}
