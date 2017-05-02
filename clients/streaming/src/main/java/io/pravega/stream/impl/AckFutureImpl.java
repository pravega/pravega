/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl;

import io.pravega.stream.AckFuture;
import com.google.common.util.concurrent.AbstractFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class AckFutureImpl extends AbstractFuture<Void> implements AckFuture {

    private final Runnable flush;

    public AckFutureImpl(CompletableFuture<Boolean> result, Runnable flush) {
        this.flush = flush;
        result.handle((bool, exception) -> {
            if (exception != null) {
                this.setException(exception);
            } else {
                if (bool) {
                    this.set(null);
                } else {
                    this.setException(new IllegalStateException("Condition failed for non-conditional write!?"));
                }
            }
            return null;
        });
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
        flushIfNeeded();
        return super.get(timeout, unit);
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        flushIfNeeded();
        return super.get();
    }

    private void flushIfNeeded() {
        if (!this.isDone()) {
            flush.run();
        }
    }

}
