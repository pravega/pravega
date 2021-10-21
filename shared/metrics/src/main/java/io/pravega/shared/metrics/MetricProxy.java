/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.metrics;

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Meter;
import io.pravega.common.Exceptions;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Getter;

/**
 * Base class for a Metric Proxy.
 *
 * @param <MetricT> Type of Metric.
 */
abstract class MetricProxy<MetricT extends Metric, SelfT extends MetricProxy<MetricT, SelfT>> implements AutoCloseable {
    private final AtomicReference<MetricT> instance = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    @Getter
    private final String proxyName;
    private final Consumer<SelfT> closeCallback;

    /**
     * Creates a new instance of the MetricProxy class.
     *
     * @param instance      The initial Metric Instance.
     * @param proxyName     The name of the MetricProxy. This may be different from the name of the Metric's instance.
     * @param closeCallback A Consumer that will be invoked when this Proxy is closed.
     */
    MetricProxy(MetricT instance, String proxyName, Consumer<SelfT> closeCallback) {
        this.closeCallback = Preconditions.checkNotNull(closeCallback, "closeCallback");
        this.proxyName = Exceptions.checkNotNullOrEmpty(proxyName, "name");
        updateInstance(instance);
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            MetricT i = this.instance.get();
            if (i != null) {
                i.close();
                this.closeCallback.accept(getSelf());
            }
        }
    }

    /**
     * All implementations should return 'this'. (Workaround to Java's lack of variance)
     */
    protected abstract SelfT getSelf();
    
    /**
     * Gets the id of the underlying metric.
     *
     * @return The id of the underlying metric.
     */
    public Meter.Id getId() {
        return getInstance().getId();
    }

    /**
     * Updates the underlying Metric instance with the given one, and closes out the previous one.
     *
     * @param newInstance The instance to update to.
     */
    void updateInstance(MetricT newInstance) {
        MetricT oldInstance = this.instance.getAndSet(Preconditions.checkNotNull(newInstance, "instance"));
        if (oldInstance != null && !newInstance.equals(oldInstance)) {
            oldInstance.close();
        }
    }

    protected MetricT getInstance() {
        return this.instance.get();
    }
}
