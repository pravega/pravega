/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;


import io.pravega.client.stream.AckFuture;
import com.google.common.util.concurrent.AbstractFuture;

import java.util.concurrent.CompletableFuture;

/**
 * Mock AckFuture.
 */
public class AckFutureMock extends AbstractFuture<Boolean> implements AckFuture {
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

