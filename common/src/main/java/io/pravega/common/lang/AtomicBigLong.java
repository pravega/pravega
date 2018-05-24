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
 * This class provides the ability to atomically update a BigLong value.
 */
public class AtomicBigLong {
    @GuardedBy("lock")
    private BigLong value;
    private final Object lock = new Object();

    public AtomicBigLong() {
        this.value = BigLong.ZERO;
    }

    @VisibleForTesting
    AtomicBigLong(int msb, long lsb) {
        this.value = new BigLong(msb, lsb);
    }

    public BigLong get() {
        synchronized (lock) {
            return this.value;
        }
    }

    public BigLong incrementAndGet() {
        synchronized (lock) {
            this.value = BigLong.add(this.value, 1);
            return this.value;
        }
    }

    public void set(int msb, long lsb) {
        synchronized (lock) {
            value = new BigLong(msb, lsb);
        }
    }
}