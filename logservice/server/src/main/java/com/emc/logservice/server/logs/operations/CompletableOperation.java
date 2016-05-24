package com.emc.logservice.server.logs.operations;

import com.emc.logservice.server.core.CallbackHelpers;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Binds a Operation with success and failure callbacks that will be invoked based on its outcome..
 */
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
     */
    public CompletableOperation(Operation operation, CompletableFuture<Long> callbackFuture) {
        this(operation, callbackFuture::complete, callbackFuture::completeExceptionally);

        if (callbackFuture.isDone()) {
            throw new IllegalArgumentException("CallbackFuture is already done.");
        }
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
        if (operation == null) {
            throw new NullPointerException("operation");
        }
        this.operation = operation;
        this.failureHandler = failureHandler;
        this.successHandler = successHandler;
    }

    //endregion

    //region Properties and Operations

    /**
     * Gets a reference to the wrapped Log Operation.
     *
     * @return
     */
    public Operation getOperation() {
        return this.operation;
    }

    /**
     * Indicates that the Log Operation has completed successfully.
     */
    public void complete() {
        long seqNo = this.operation.getSequenceNumber();
        if (seqNo < 0) {
            throw new IllegalStateException("About to complete a CompletableOperation that has no sequence number.");
        }

        this.done = true;
        if (this.successHandler != null) {
            CallbackHelpers.invokeSafely(this.successHandler, seqNo, null);
        }
    }

    /**
     * Indicates that the Log Operation has failed to complete.
     *
     * @param ex The causing exception.
     */
    public void fail(Throwable ex) {
        this.done = true;
        if (this.failureHandler != null) {
            CallbackHelpers.invokeSafely(this.failureHandler, ex, null);
        }
    }

    /**
     * Gets a value indicating whether this CompletableOperation has finished, regardless of outcome.
     *
     * @return True if finished, false otherwise.
     */
    public boolean isDone() {
        return this.done;
    }

    //endregion
}
