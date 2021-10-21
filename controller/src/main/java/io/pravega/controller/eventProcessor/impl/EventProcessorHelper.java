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
package io.pravega.controller.eventProcessor.impl;

import io.pravega.common.util.Retry;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

@Slf4j
public class EventProcessorHelper {
    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 2;
    private static final int RETRY_MAX_ATTEMPTS = 5;
    private static final long RETRY_MAX_DELAY = Duration.ofSeconds(2).toMillis();

    private static final Retry.RetryAndThrowConditionally RETRY = Retry
            .withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
            .retryWhen(RetryableException::isRetryable);

    private static final Retry.RetryUnconditionally INDEFINITELY = Retry
            .indefinitelyWithExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_DELAY,
                    e -> {
                        if (log.isDebugEnabled()) {
                            log.debug("error while writing event back into stream {}", e);
                        } else {
                            log.warn("error while writing event back into stream");
                        }
                    });

    public static <U> CompletableFuture<U> withRetries(Supplier<CompletableFuture<U>> futureSupplier, ScheduledExecutorService executor) {
        return RETRY.runAsync(futureSupplier, executor);
    }

    public static <U> CompletableFuture<U> indefiniteRetries(Supplier<CompletableFuture<U>> futureSupplier, ScheduledExecutorService executor) {
        return INDEFINITELY.runAsync(futureSupplier, executor);
    }

    public static <R extends ControllerEvent> CompletableFuture<Void> writeBack(R request, EventProcessor.Writer<R> writer) {
        if (writer == null) {
            return CompletableFuture.completedFuture(null);
        }
        return writer.write(request);
    }
}
