/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.lang;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.GuardedBy;

/**
 * This class provides the ability to atomically update a Int96 value.
 */
public class AtomicInt96 {
    @GuardedBy("lock")
    private Int96 value;
    private final Object lock = new Object();

    public AtomicInt96() {
        this.value = Int96.ZERO;
    }

    @VisibleForTesting
    AtomicInt96(int msb, long lsb) {
        this.value = new Int96(msb, lsb);
    }

    public Int96 get() {
        synchronized (lock) {
            return this.value;
        }
    }

    public Int96 incrementAndGet() {
        synchronized (lock) {
            this.value = this.value.add(1);
            return this.value;
        }
    }

    public void set(int msb, long lsb) {
        synchronized (lock) {
            value = new Int96(msb, lsb);
        }
    }
}