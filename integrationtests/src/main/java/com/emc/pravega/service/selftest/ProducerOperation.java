/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.selftest;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.CallbackHelpers;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Represents an Operation for a Producer
 */
class ProducerOperation {
    //region Members

    @Getter
    private final ProducerOperationType type;
    @Getter
    private final String target;
    @Getter
    @Setter
    private Object result;
    @Getter
    @Setter
    private int length;
    @Setter
    private Consumer<ProducerOperation> completionCallback;
    @Setter
    private BiConsumer<ProducerOperation, Throwable> failureCallback;
    @Getter
    private Duration duration = Duration.ZERO;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ProducerOperation class.
     *
     * @param type   The type of the operation.
     * @param target The target (Segment name) of the operation.
     */
    ProducerOperation(ProducerOperationType type, String target) {
        Preconditions.checkNotNull(type, "type");
        Exceptions.checkNotNullOrEmpty(target, "target");
        this.type = type;
        this.target = target;
    }

    //endregion

    //region Completion

    /**
     * Indicates that this ProducerOperation completed successfully. Invokes any associated success callbacks that are
     * registered with it.
     *
     * @param duration The elapsed time for this operation.
     */
    void completed(Duration duration) {
        this.duration = duration;
        Consumer<ProducerOperation> callback = this.completionCallback;
        if (callback != null) {
            CallbackHelpers.invokeSafely(callback, this, null);
        }
    }

    /**
     * Indicates that this ProducerOperation failed to complete. Invokes any associated failure callbacks that are registered
     * with it.
     */
    void failed(Throwable ex) {
        BiConsumer<ProducerOperation, Throwable> callback = this.failureCallback;
        if (callback != null) {
            CallbackHelpers.invokeSafely(callback, this, ex, null);
        }
    }

    //endregion

    @Override
    public String toString() {
        return String.format("%s: %s", this.type, this.target);
    }
}
