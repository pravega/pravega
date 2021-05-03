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

import java.util.Collection;

/**
 * The top level interface used to provide any and all health related information for a particular component
 * of Pravega. It holds the {@link ContributorRegistry} and provides the {@link HealthEndpoint} used to make health
 * information accessible to clients.
 */
public interface HealthService extends AutoCloseable {

    /**
     * Returns a {@link Collection} of all the components the {@link HealthService} is responsible for and observes.
     * @return
     */
    Collection<String> components();

    /**
     * The {@link ContributorRegistry} acts as the means to organize and references to the various {@link HealthContributor}
     * objects we wish to track.
     *
     * @return The {@link ContributorRegistry} backing the {@link HealthService}.
     */
    ContributorRegistry registry();

    /**
     * The main interface between some client and the {@link HealthService}. The {@link HealthEndpoint} encapsulates
     * the various types of requests the {@link HealthService} will be able to fulfill.
     *
     * @return The {@link HealthEndpoint} instance.
     */
    HealthEndpoint endpoint();

    /**
     * Provides the {@link HealthServiceUpdater} that currently performs the background {@link Health} requests (of the root).
     * @return The  {@link HealthServiceUpdater}.
     */
    HealthServiceUpdater getHealthServiceUpdater();

    /**
     * Provides the name assigned to this {@link HealthService}. This name will also act as the name for the root
     * {@link HealthContributor} of it's {@link ContributorRegistry}.
     *
     * @return The name of the service.
     */
    String getName();

    /**
     * Removes all state (contents of the {@link ContributorRegistry}) from this object.
     */
    void clear();

    @Override
    void close();
}
