/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
