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
package io.pravega.shared.health;

import io.pravega.common.Exceptions;
import lombok.NonNull;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides instances of a {@link HealthServiceManager} and optionally starts the {@link HealthServiceUpdater}.
 */
public class HealthServiceFactory implements AutoCloseable {
    private final AtomicBoolean closed;

    /**
     * Creates a new instances of the {@link HealthServiceFactory}.
     */
    public HealthServiceFactory() {
        this.closed = new AtomicBoolean();
    }

    /**
     * Provides an instance of the {@link HealthServiceManager} and may optionally start its {@link HealthServiceUpdater}.
     * @param interval The interval at which the 'health tree' is updated.
     *
     * @return The created {@link HealthServiceManager} instance.
     */
    @NonNull
    public HealthServiceManager createHealthService(Duration interval) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        HealthServiceManager service = new HealthServiceManager( interval);
        return service;
    }

    public HealthServiceManager createHealthService() {
        return createHealthService(Duration.ofSeconds(10));
    }

    /**
     * Closes the {@link HealthServiceFactory} instance, making it unable to create further {@link HealthServiceManager} instances.
     */
    @Override
    public void close() {
        this.closed.set(true);
    }
}
