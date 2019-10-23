/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;

public class LimitedConcurrentSkipListMap<K, V>
extends ConcurrentSkipListMap<K, V>
implements LimitedNavigableMap<K, V> {

    private final int sizeLimit;
    private final Semaphore insertPermits;

    public LimitedConcurrentSkipListMap(final int sizeLimit) {
        this.sizeLimit = sizeLimit;
        this.insertPermits = new Semaphore(sizeLimit, true);
    }

    @Override
    public final int size() {
        return sizeLimit - insertPermits.availablePermits();
    }

    @Override
    public final boolean isEmpty() {
        return sizeLimit == insertPermits.availablePermits();
    }

    @Override
    public final boolean putIfNotFull(final K k, final V v) {
        if (insertPermits.tryAcquire()) {
            super.put(k, v);
            return true;
        }
        return false;
    }

    @Override
    public final V remove(final Object o) {
        insertPermits.release();
        return super.remove(o);
    }

    @Override
    public final boolean remove(final Object o, final Object o1) {
        insertPermits.release();
        return super.remove(o, o1);
    }

    @Override
    public final void clear() {
        insertPermits.release(sizeLimit);
        super.clear();
    }
}
