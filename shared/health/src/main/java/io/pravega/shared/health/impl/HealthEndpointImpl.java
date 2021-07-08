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
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.HealthServiceUpdater;
import io.pravega.shared.health.ContributorNotFoundException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.pravega.shared.health.HealthContributor.DELIMITER;

/**
 * Provides a simpler interface for retrieving {@link Health} information from the {@link io.pravega.shared.health.HealthServiceManager}.
 * The {@link HealthEndpoint} should be used to serve information about the {@link Health} of a component for external classes.
 *
 * The 'String id' parameter of the following methods si expected to be a fully qualified id, i.e. containing the complete path
 * (delimited by {@link HealthContributor#DELIMITER}) starting from the HealthServiceManager.RootHealthContributor.
 */
@Slf4j
@RequiredArgsConstructor
public class HealthEndpointImpl implements HealthEndpoint {

    @NonNull
    private final HealthContributor root;

    @NonNull
    private final HealthServiceUpdater updater;

    /**
     * Provides the name of the root {@link HealthContributor}.
     * @return The name (its id).
     */
    @Override
    public String getDefaultContributorName() {
        return root.getName();
    }

    /**
     * Validates that the {@link HealthContributor} exists and requests it's {@link Health}.
     *
     * @param id  The id/name of the {@link HealthContributor} to check the {@link Health} of.
     * @return The {@link Health} result of the {@link HealthContributor} with name 'id'. Returns NULL if 'id' does not map to
     * a {@link HealthContributor}.
     */
    @NonNull
    @Override
    public Health getHealth(String id) {
        Health health;
        if (id == null || id.equals(root.getName())) {
            health = updater.getLatestHealth();
        } else {
            List<String> path = Arrays.asList(id.split(DELIMITER));
            health = search(path, updater.getLatestHealth());
        }
        if (health == null)  {
            throw new ContributorNotFoundException(String.format("No HealthContributor found with name '%s'", id), 404);
        }
        return health;
    }

    @Override
    public Health getHealth() {
        return getHealth(root.getName());
    }

    /**
     * Provides the health {@link Status} for the {@link HealthContributor} with name 'id'.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The {@link Status} result.
     */
    @Override
    public Status getStatus(String id) {
        return getHealth(id).getStatus();
    }

    /**
     * Provides the readiness status of the {@link HealthContributor} with name 'id'.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The readiness result.
     */
    @Override
    public boolean isReady(String id) {
        return getHealth(id).isReady();
    }

    /**
     * Provides the liveness status of the {@link HealthContributor} with name 'id'.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The liveness result.
     */
    @Override
    public boolean isAlive(String id) {
        return getHealth(id).isAlive();
    }

    /**
     * Provides a {@link Map} of the {@link Objects} used to create a {@link  HealthContributor} details response.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The {@link Map}.
     */
    @Override
    public Map<String, Object> getDetails(String id) {
        return getHealth(id).getDetails();
    }

    // Performs a linear search over the 'health tree'.
    private Health search(List<String> path, Health current) {
        Health child = current;
        for (String id : path) {
            child = current.getChildren().get(id);
            if (child != null)  {
                current = child;
            } else {
                return null;
            }
        }
        return child;
    }
}

