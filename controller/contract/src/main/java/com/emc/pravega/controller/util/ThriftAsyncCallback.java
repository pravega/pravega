/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.util;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.concurrent.CompletableFuture;

/**
 * Thrift AsyncCallback implementation using CompletableFuture.
 */
public class ThriftAsyncCallback<T> implements AsyncMethodCallback<T> {

    private final CompletableFuture<T> result = new CompletableFuture<>();

    @Override
    public void onComplete(T response) {
       result.complete(response);
    }

    @Override
    public void onError(Exception exception) {
        result.completeExceptionally(exception);
    }

    public CompletableFuture<T> getResult() {
        return result;
    }
}
