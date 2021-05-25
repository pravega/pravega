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
package io.pravega.client.control.impl;

import io.pravega.common.concurrent.Futures;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class CancellableRequest<T> {
    private final AtomicBoolean done;
    private final AtomicBoolean cancelled;
    private final AtomicReference<T> result;
    private final AtomicReference<CompletableFuture<T>> futureRef;
    private final CompletableFuture<Void> started;

    public CancellableRequest() {
        cancelled = new AtomicBoolean(false);
        done = new AtomicBoolean(false);
        result = new AtomicReference<>();
        futureRef = new AtomicReference<>(null);
        started = new CompletableFuture<>();
    }

    public void cancel() {
        cancelled.set(true);
        futureRef.updateAndGet(future -> {
            if (future != null) {
                future.cancel(true);
            }
            return future;
        });
    }

    public void start(Supplier<CompletableFuture<T>> supplier, Predicate<T> termination, ScheduledExecutorService executor) {
        futureRef.updateAndGet(previous -> {
            if (previous != null) {
                throw new IllegalStateException("Request already started");
            }

            return Futures.loop(() -> !done.get() && !cancelled.get(),
                    () -> Futures.delayedFuture(() -> supplier.get().thenAccept(r -> {
                        result.set(r);
                        done.set(termination.test(r));
                    }), 1000, executor), executor)
                          .thenApply((Void v) -> {
                        if (done.get()) { // completed
                            return result.get();
                        } else { // cancelled
                            throw new CancellationException();
                        }
                    });
        });

        started.complete(null);
    }

    public CompletableFuture<T> getFuture() {
        return started.thenCompose(x -> futureRef.get());
    }
}
