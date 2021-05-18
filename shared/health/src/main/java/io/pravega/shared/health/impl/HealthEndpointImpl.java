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
     * @param includeDetails Whether or not to include detailed information provided by said {@link HealthContributor}.
     * @return The {@link Health} result of the {@link HealthContributor} with name 'id'. Returns NULL if 'id' does not map to
     * a {@link HealthContributor}.
     */
    @NonNull
    @Override
    public Health getHealth(String id, boolean includeDetails) {
        HealthContributor result = search(root, id);
        return result == null ? null : result.getHealthSnapshot();
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
        return getHealth(id, true).getDetails();
    }

    // Performs a basic recursive dfs to find if there exists some HealthContributor with name 'id' that is reachable
    // from a given HealthContributor.
    private HealthContributor search(HealthContributor current, String id) {
        if (current.getName() == id) {
            return current;
        }

        for (Map.Entry<String, HealthContributor> entry: current.getContributors().entrySet()) {
            HealthContributor contributor = entry.getValue();
            if (search(contributor, id) != null) {
                return contributor;
            }
        }

        return null;
    }
}

