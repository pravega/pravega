/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.mocks;


import com.emc.pravega.stream.AckFuture;
import com.google.common.util.concurrent.AbstractFuture;

import java.util.concurrent.CompletableFuture;

/**
 * Mock AckFuture.
 */
public class AckFutureMock extends AbstractFuture<Void> implements AckFuture {
    public AckFutureMock(CompletableFuture<Boolean> result) {
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
}

