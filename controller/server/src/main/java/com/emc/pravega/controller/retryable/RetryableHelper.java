/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.retryable;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;
import com.emc.pravega.controller.store.host.HostStoreException;
import com.emc.pravega.controller.store.stream.StoreConnectionException;
import com.emc.pravega.controller.store.stream.TransactionBlockedException;
import com.emc.pravega.controller.store.stream.WriteConflictException;
import com.emc.pravega.controller.store.task.ConflictingTaskException;
import com.emc.pravega.controller.store.task.LockFailedException;
import com.emc.pravega.controller.store.task.UnlockFailedException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class RetryableHelper {
    private static final HashSet<Class<? extends Exception>> RETRIABLES = Sets.newHashSet(
            LockFailedException.class,
            HostStoreException.class,
            ConflictingTaskException.class,
            StoreConnectionException.class,
            TransactionBlockedException.class,
            UnlockFailedException.class,
            WireCommandFailedException.class,
            WriteConflictException.class);

    private static boolean isRetryable(Throwable e) {
        Throwable cause = ExceptionHelpers.getRealException(e);

        return RETRIABLES.stream().anyMatch(retryable -> cause.getCause().getClass().isAssignableFrom(retryable));
    }

    public static Optional<RetryableException> getRetryable(Throwable e) {
        Preconditions.checkNotNull(e);
        Throwable cause = ExceptionHelpers.getRealException(e);
        if (cause != null && isRetryable(cause)) {
            return Optional.of(new RetryableException(cause));
        } else {
            return Optional.empty();
        }
    }

    public static <T> CompletableFuture<T> transformWithRetryable(CompletableFuture<T> future) {
        CompletableFuture<T> result = new CompletableFuture<T>();
        return future.whenComplete((r, e) -> {
            if (e != null) {
                Optional<RetryableException> retryable = getRetryable(e);
                if (retryable.isPresent()) {
                    result.completeExceptionally(retryable.get());
                } else {
                    result.completeExceptionally(e);
                }
            } else {
                result.complete(r);
            }
        });
    }

}
