/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.CallbackHelpers;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Binds a Operation with success and failure callbacks that will be invoked based on its outcome..
 */
@Slf4j
public class CompletableOperation {
    //region Members

    private final Operation operation;
    private final Consumer<Throwable> failureHandler;
    private final Consumer<Long> successHandler;
    private boolean done;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CompletableOperation class.
     *
     * @param operation      The operation to wrap.
     * @param callbackFuture A CompletableFuture that will be used to indicate the outcome of this operation.
     *                       If successful, the CompletableFuture will contain the Sequence Number of the Operation as its payload.
     * @throws IllegalArgumentException If the given callbackFuture is already done.
     */
    public CompletableOperation(Operation operation, CompletableFuture<Long> callbackFuture) {
        this(operation, callbackFuture::complete, callbackFuture::completeExceptionally);
        Exceptions.checkArgument(!callbackFuture.isDone(), "callbackFuture", "CallbackFuture is already done.");
    }

    /**
     * Creates a new instance of the CompletableOperation class.
     *
     * @param operation      The operation to wrap.
     * @param successHandler A consumer that will be invoked if this operation is successful. The argument provided is the Sequence Number of the Operation.
     * @param failureHandler A consumer that will be invoked if this operation failed. The argument provided is the causing Exception for the failure.
     * @throws NullPointerException If operation is null.
     */
    public CompletableOperation(Operation operation, Consumer<Long> successHandler, Consumer<Throwable> failureHandler) {
        Preconditions.checkNotNull(operation, "operation");
        this.operation = operation;
        this.failureHandler = failureHandler;
        this.successHandler = successHandler;
    }

    //endregion

    //region Properties

    /**
     * Gets a reference to the wrapped Log Operation.
     */
    public Operation getOperation() {
        return this.operation;
    }

    /**
     * Completes the operation (no exception).
     */
    public void complete() {
        long seqNo = this.operation.getSequenceNumber();
        Preconditions.checkState(seqNo >= 0, "About to complete a CompletableOperation that has no sequence number.");

        this.done = true;
        if (this.successHandler != null) {
            CallbackHelpers.invokeSafely(this.successHandler, seqNo, cex -> log.error("Success Callback invocation failure.", cex));
        }
    }

    /**
     * Completes the operation with failure.
     *
     * @param ex The causing exception.
     */
    public void fail(Throwable ex) {
        this.done = true;
        if (this.failureHandler != null) {
            CallbackHelpers.invokeSafely(this.failureHandler, ex, cex -> log.error("Fail Callback invocation failure.", cex));
        }
    }

    /**
     * Gets a value indicating whether this operation has finished, regardless of outcome.
     *
     * @return True if finished, false otherwise.
     */
    public boolean isDone() {
        return this.done;
    }

    //endregion
}
