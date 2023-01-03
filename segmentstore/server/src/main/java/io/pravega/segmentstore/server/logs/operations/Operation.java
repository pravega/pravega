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
package io.pravega.segmentstore.server.logs.operations;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.contracts.SequencedElement;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * Base class for a Log Operation.
 */
public abstract class Operation implements SequencedElement {
    //region Members

    public static final long NO_SEQUENCE_NUMBER = Long.MIN_VALUE;
    private long sequenceNumber;
    /**
     * Requested priority. This field is not serialized.
     */
    @Getter
    @Setter
    private OperationPriority desiredPriority;

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
     * Gets a value indicating the number of bytes that this operation requires in the cache.
     *
     * @return The number of bytes required, or 0 if it doesn't involve any cache operations.
     */
    public long getCacheLength() {
        return 0;
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
     * Allows to overwrite the Sequence Number of this operation. This may be a dangerous action and should be used
     * carefully only for admin purposes when repairing a log of Operations is needed.
     *
     * @param value The Sequence Number to set.
     * @throws IllegalArgumentException If the Sequence Number is negative.
     */
    @VisibleForTesting
    public void resetSequenceNumber(long value) {
        Exceptions.checkArgument(value >= 0, "value", "Sequence Number must be a non-negative number.");
        this.sequenceNumber = value;
    }

    /**
     * Gets a value indicating the type of this operation.
     *
     * @return The {@link OperationType}.
     */
    public OperationType getType() {
        return OperationType.Normal;
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

    /**
     * Base class for any Operation Serializer.
     * @param <T> Operation Type.
     */
    protected static abstract class OperationSerializer<T extends Operation> extends VersionedSerializer.WithBuilder<T, OperationBuilder<T>> {
        @Override
        protected void beforeSerialization(T operation) {
            Preconditions.checkState(operation.getSequenceNumber() >= 0, "Sequence Number has not been assigned.");
        }
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
