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

import java.util.Map;

/**
 * The {@link HealthEndpoint} interface defines all the types of requests a {@link HealthServiceManager} should expect to serve.
 */
public interface HealthEndpoint {

    /**
     * Provides the name of the default {@link HealthContributor} that all other {@link HealthContributor} objects are
     * reachable from.
     *
     * @return The name/id of the default {@link HealthContributor}.
     */
    String getDefaultContributorName();

    /**
     * Provides the {@link Status} of the {@link HealthContributor} with name {@link #getDefaultContributorName()}.
     *
     * @return The {@link Status} of the default {@link HealthContributor}.
     */
    default Status getStatus() {
        return getStatus(getDefaultContributorName());
    }

    /**
     * Fetches the {@link Status} of the {@link HealthContributor} mapped to by 'id'.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The current {@link Status} of the {@link HealthContributor}.
     */
    Status getStatus(String id);

    /**
     * Fetches the {@link Status} of the 'root' {@link HealthContributor}.
     *
     * @return The current {@link Status} of the 'root' {@link HealthContributor}.
     */
    default boolean isReady() {
        return isReady(getDefaultContributorName());
    }

    /**
     * Determine if some {@link HealthContributor} object is currently in a 'ready' state.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The 'readiness' result.
     */
    boolean isReady(String id);

    /**
     * Determine if the root {@link HealthContributor} object is currently in an 'alive' state.
     *
     * @return The 'liveness' result.
     */
    default boolean isAlive() {
        return isAlive(getDefaultContributorName());
    }

    /**
     * Determine if some {@link HealthContributor} object is currently in an 'alive' state.
     * @param id The id of some {@link HealthContributor} to search for.
     *
     * @return The 'liveness' result.
     */
    boolean isAlive(String id);

    /**
     * Fetches the details from the root {@link HealthContributor}. This will always return a {@link Map}
     * object with no entries.
     *
     * @return The {@link Map} of details results.
     */
    default Map<String, Object> getDetails() {
        return getDetails(getDefaultContributorName());
    }

    /**
     * Fetches the results from the list of {@link java.util.function.Supplier} provided during {@link HealthContributor}
     * construction.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The {@link Map} of details results.
     */
    Map<String, Object> getDetails(String id);

    /**
     * Requests the {@link Health} result for some {@link HealthContributor}.
     * @param id  The id/name of the {@link HealthContributor} to check the {@link Health} of.
     * @return The {@link Health} object of the {@link HealthContributor}.
     */
    Health getHealth(String id);

    /**
     * Requests the {@link Health} for the service level (root) {@link HealthContributor}.
     * @return The {@link Health} of the whole health service.
     */
    Health getHealth();

}
