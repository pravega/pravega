/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package io.pravega.shared.metrics;

import java.util.concurrent.atomic.AtomicReference;

public class CounterProxy implements Counter {
    private final AtomicReference<Counter> instance = new AtomicReference<>();

    CounterProxy(Counter counter) {
        instance.set(counter);
    }

    void setCounter(Counter counter) {
        instance.set(counter);
    }

    @Override
    public void clear() {
        instance.get().clear();
    }

    @Override
    public void inc() {
        instance.get().inc();
    }

    @Override
    public void dec() {
        instance.get().dec();
    }

    @Override
    public void add(long delta) {
        instance.get().add(delta);
    }

    @Override
    public long get() {
        return instance.get().get();
    }

    @Override
    public String getName() {
        return instance.get().getName();
    }
}
