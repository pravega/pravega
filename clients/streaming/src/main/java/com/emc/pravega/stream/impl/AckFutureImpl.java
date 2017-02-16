package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.AckFuture;
import com.google.common.util.concurrent.AbstractFuture;

import java.util.concurrent.CompletableFuture;

final class AckFutureImpl extends AbstractFuture<Void> implements AckFuture {

    public AckFutureImpl(CompletableFuture<Boolean> result) {
        result.handle((bool, exception) -> {
            if (exception != null) {
                this.setException(exception);
            }
            if (bool == true) {
                this.set(null);
            } else {
                this.setException(new IllegalStateException("Condition failed for non-conditional write!?"));
            }
            return null;
        });
    }

}
