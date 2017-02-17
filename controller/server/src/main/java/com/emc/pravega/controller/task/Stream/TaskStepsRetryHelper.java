/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.emc.pravega.controller.util.ExceptionHelper.extractCause;

class TaskStepsRetryHelper {
    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 2;
    private static final int RETRY_MAX_ATTEMPTS = 100;
    private static final long RETRY_MAX_DELAY = Duration.ofSeconds(10).toMillis();
    private static final Retry.RetryAndThrowExceptionally<RetryableException, RuntimeException> RETRY = Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
            .retryingOn(RetryableException.class)
            .throwingOn(RuntimeException.class);

    static <U> CompletableFuture<U> withRetries(Supplier<CompletableFuture<U>> future, ScheduledExecutorService executor) {
        return RETRY.runAsync(future, executor);
    }

    static <U> CompletableFuture<U> withWireCommandHandling(CompletableFuture<U> future) {
        return future.handle((res, ex) -> {
            if (ex != null) {
                Throwable cause = extractCause(ex);
                if (cause instanceof WireCommandFailedException) {
                    throw (WireCommandFailedException) cause;
                } else {
                    throw new RuntimeException(ex);
                }
            }
            return res;
        });
    }
}
