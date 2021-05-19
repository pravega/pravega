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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
public class HealthEndpointImpl implements HealthEndpoint {

    @NonNull
    private final HealthContributor root;

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
        Health health = search(id, root.getHealthSnapshot());
        if (health == null) {
            throw new ContributorNotFoundException();
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

    // Perform a basic DFS over the all health nodes reachable from the root.
    Health search(String id, Health current) {
        if (id == current.getName()) {
            return current;
        }

        for (val child : current.getChildren()) {
            Health health = search(id, child);
            if (health != null)  {
                return health;
            }
        }

        return null;
    }

    /**
     * Exception thrown when the {@link HealthEndpoint} is called given some {@link HealthContributor} name and
     * no such {@link HealthContributor} could be found.
     */
    public class ContributorNotFoundException extends RuntimeException {
    }
}

