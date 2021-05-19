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
package io.pravega.shared.health.impl;

import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.HealthServiceUpdater;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.HealthService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class HealthServiceImpl implements HealthService {

    /**
     * The root {@link HealthContributor} of the service. All {@link HealthContributor} objects are reachable from this
     * contributor.
     */
    @Getter
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

    @Getter
    private final String name;

    public HealthServiceImpl(String name) {
        this.name = name;
        this.root = new RootHealthContributor();
        this.endpoint = new HealthEndpointImpl(this.root);
        // Initializes the ContributorRegistry into the expected starting state.
        this.closed = new AtomicBoolean();
        this.updater = new HealthServiceUpdaterImpl(this);
    }

    @Override
    public HealthEndpoint getEndpoint() {
        return this.endpoint;
    }

    @Override
    public HealthServiceUpdater getHealthServiceUpdater() {
        return updater;
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.updater.close();
            this.root.close();
        }
    }

    class RootHealthContributor extends HealthContributorImpl {

        RootHealthContributor() {
            super(name, StatusAggregatorImpl.UNANIMOUS);
        }

        @Override
        public Status doHealthCheck(Health.HealthBuilder builder) throws Exception {
            Status status = Status.UP;
            builder.status(status);
            return status;
        }
    }
}
