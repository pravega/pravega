package com.emc.logservice.server.core;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Provides a mechanism for executing asynchronous methods atomically (sequentially).
 * No two such method invocations will execute in parallel.
 */
public class AsyncLock {
    public <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> methodSupplier) {
        //TODO: implement properly, including catching exceptions and handling them.
        return methodSupplier.get();
    }
}
