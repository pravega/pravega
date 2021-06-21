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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import io.pravega.shared.health.impl.HealthEndpointImpl;
import io.pravega.shared.health.impl.HealthServiceUpdaterImpl;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class HealthServiceManager implements AutoCloseable {

    /**
     * The root {@link HealthContributor} of the service. All {@link HealthContributor} objects are reachable from this
     * contributor.
     */
    @Getter
    @VisibleForTesting
    private final HealthContributor root;

    /**
     * The {@link HealthServiceUpdater} which provides passive health updates.
     */
    private final HealthServiceUpdater updater;

    /**
     * The flag used to protect against concurrent {@link #close()} calls.
     */
    private final AtomicBoolean closed;

    private final HealthEndpoint endpoint;

    public HealthServiceManager(Duration interval) {
        this.root = new RootHealthContributor();
        this.closed = new AtomicBoolean();
        this.updater = new HealthServiceUpdaterImpl(this.root, interval);
        this.endpoint = new HealthEndpointImpl(this.root, this.updater);
    }

    /**
     * The main interface between some client and the {@link HealthServiceManager}. The {@link HealthEndpoint} encapsulates
     * the various types of requests the {@link HealthServiceManager} will be able to fulfill.
     *
     * @return The {@link HealthEndpoint} instance.
     */
    public HealthEndpoint getEndpoint() {
        return this.endpoint;
    }

    @VisibleForTesting
    HealthServiceUpdater getHealthServiceUpdater() {
        return updater;
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.updater.close();
            this.root.close();
        }
    }

    private static class RootHealthContributor extends AbstractHealthContributor {

        RootHealthContributor() {
            super("", StatusAggregator.UNANIMOUS);
        }

        @Override
        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.UP;
            builder.status(status);
            return status;
        }
    }
}
