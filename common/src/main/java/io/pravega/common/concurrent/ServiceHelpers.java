/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.common.concurrent;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Service;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * TODO: get rid of ServiceShutdownListener, or at least the static methods.
 * TODO: proper javadoc and unit test.
 * TODO: handle race conditions when we register a listener but the service is already in that state, or is getting to that state concurrently.
 */
public final class ServiceHelpers {

    public static CompletableFuture<Void> startAsync(Service service, Executor executor) {
        Preconditions.checkState(service.state() != Service.State.RUNNING, "Service is already in a %s state.", Service.State.RUNNING);
        CompletableFuture<Void> result = new CompletableFuture<>();
        service.addListener(new StartupListener(result), executor);
        service.startAsync();
        return result;
    }

    public static CompletableFuture<Void> stopAsync(Service service, Executor executor) {
        if (service.state() == Service.State.FAILED) {
            return FutureHelpers.failedFuture(service.failureCause());
        } else if (service.state() == Service.State.TERMINATED) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        service.addListener(new ShutdownListener(result), executor);
        service.stopAsync();
        return result;
    }

    @RequiredArgsConstructor
    private static class ShutdownListener extends Service.Listener {
        private final CompletableFuture<Void> completion;

        @Override
        public void terminated(@Nonnull Service.State from) {
            this.completion.complete(null);
        }

        @Override
        public void failed(@Nonnull Service.State from, @Nonnull Throwable failure) {
            this.completion.completeExceptionally(failure);
        }
    }

    @RequiredArgsConstructor
    private static class StartupListener extends Service.Listener {
        private final CompletableFuture<Void> completion;

        @Override
        public void running() {
            this.completion.complete(null);
        }

        @Override
        public void terminated(@Nonnull Service.State from) {
            this.completion.completeExceptionally(new IllegalStateException(
                    String.format("Service expected to be %s but was %s.", Service.State.RUNNING, Service.State.TERMINATED)));
        }

        @Override
        public void failed(@Nonnull Service.State from, @Nonnull Throwable failure) {
            this.completion.completeExceptionally(failure);
        }
    }
}
