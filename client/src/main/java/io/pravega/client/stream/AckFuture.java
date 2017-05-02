/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public interface AckFuture extends Future<Void> {
    
    /**
     * Adds a callback that will be invoked in a thread in the provided executor once this future has completed. 
     * IE: once {@link #get()} can be called without blocking.
     * 
     * @param callback The function to be run once a result is available.
     * @param executor The executor to run the callback.
     */
    void addListener(Runnable callback, Executor executor);
}
