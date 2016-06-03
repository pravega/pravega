package com.emc.logservice.common;

import java.util.concurrent.CompletableFuture;

/**
 * Extensions to Future and CompletableFuture.
 */
public class FutureHelpers {
    /**
     * Creates a new CompletableFuture that is failed with the given exception.
     * @param exception The exception to fail the CompletableFuture.
     * @param <T>
     * @return
     */
    public static <T> CompletableFuture<T> failedFuture(Throwable exception) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(exception);
        return result;
    }
}
