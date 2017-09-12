/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.shared.metrics;

import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Base class for a Metric Proxy.
 *
 * @param <T> Type of Metric.
 */
abstract class MetricProxy<T extends Metric> implements AutoCloseable {
    private final AtomicReference<T> instance = new AtomicReference<>();
    private final Consumer<String> closeCallback;

    /**
     * Creates a new instance of the MetricProxy class.
     *
     * @param instance      The initial Metric Instance.
     * @param closeCallback A Consumer that will be invoked when this Proxy is closed.
     */
    MetricProxy(T instance, Consumer<String> closeCallback) {
        this.closeCallback = Preconditions.checkNotNull(closeCallback, "closeCallback");
        updateInstance(instance);
    }

    @Override
    public void close() {
        T i = getInstance();
        if (i != null) {
            i.close();
            this.closeCallback.accept(i.getName());
        }
    }

    public String getName() {
        return getInstance().getName();
    }

    void updateInstance(T instance) {
        T oldInstance = this.instance.getAndSet(Preconditions.checkNotNull(instance, "instance"));
        if (oldInstance != null) {
            oldInstance.close();
        }
    }

    protected T getInstance() {
        return this.instance.get();
    }
}
