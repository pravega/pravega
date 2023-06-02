package io.pravega.common.concurrent;

import java.util.concurrent.CompletableFuture;

/**
 * A function which returns a future.
 */
@FunctionalInterface
public interface FutureSuplier<T> {

    /**
     * @return A CompletableFuture
     */
    CompletableFuture<T> getFuture();
    
}
