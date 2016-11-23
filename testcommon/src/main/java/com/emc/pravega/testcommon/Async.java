/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.testcommon;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;

public class Async {

    public static <ResultT> ResultT testBlocking(Callable<ResultT> blockingFunction, Runnable unblocker) {
        final AtomicReference<ResultT> result = new AtomicReference<>(null);
        final AtomicReference<Exception> exception = new AtomicReference<>(null);
        final Semaphore isBlocked = new Semaphore(0);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    result.set(blockingFunction.call());
                } catch (Exception e) {
                    exception.set(e);
                }
                isBlocked.release();
            }
        });
        t.start();
        try {
            Assert.assertFalse(isBlocked.tryAcquire(100, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        unblocker.run();
        try {
            isBlocked.acquire();
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (exception.get() != null) {
            throw new RuntimeException(exception.get());
        } else {
            return result.get();
        }
    }

}
