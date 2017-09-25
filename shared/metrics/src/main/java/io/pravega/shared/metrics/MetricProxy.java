/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Getter;

/**
 * Base class for a Metric Proxy.
 *
 * @param <T> Type of Metric.
 */
abstract class MetricProxy<T extends Metric> implements AutoCloseable {
    private final AtomicReference<T> instance = new AtomicReference<>();
    @Getter
    private final String proxyName;
    private final Consumer<String> closeCallback;

    /**
     * Creates a new instance of the MetricProxy class.
     *
     * @param instance      The initial Metric Instance.
     * @param proxyName     The name of the MetricProxy. This may be different from the name of the Metric's instance.
     * @param closeCallback A Consumer that will be invoked when this Proxy is closed.
     */
    MetricProxy(T instance, String proxyName, Consumer<String> closeCallback) {
        this.closeCallback = Preconditions.checkNotNull(closeCallback, "closeCallback");
        this.proxyName = Exceptions.checkNotNullOrEmpty(proxyName, "name");
        updateInstance(instance);
    }

    @Override
    public void close() {
        T i = getInstance();
        if (i != null) {
            i.close();
            this.closeCallback.accept(this.proxyName);
        }
    }

    /**
     * Gets the name of the underlying metric.
     *
     * @return The name.
     */
    public String getName() {
        return getInstance().getName();
    }

    /**
     * Updates the underlying Metric instance with the given one, and closes out the previous one.
     *
     * @param instance The instance to update to.
     */
    void updateInstance(T instance) {
        T oldInstance = this.instance.getAndSet(Preconditions.checkNotNull(instance, "instance"));
        if (oldInstance != null && oldInstance != instance) {
            oldInstance.close();
        }
    }

    protected T getInstance() {
        return this.instance.get();
    }
}
