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
import java.util.Collections;
import java.util.Map;

/**
 * A {@link HealthContributor} is an interface that is able to provide or *contribute* health information relating to
 * an arbitrary component, process, object, etc.
 */
public interface HealthContributor {

    /**
     * The list of {@link HealthContributor} objects that this {@link HealthContributor} object depends on to determine
     * its own health.
     *
     * @return The list of {@link HealthContributor} contributing to this {@link HealthContributor}.
     */
    default Map<String, HealthContributor> getContributors() {
        return Collections.emptyMap();
    }

    /**
     * From an abstract view, a {@link HealthContributor} is anything that has an impact on the health of the system.
     * As such it should provide a window into it's current state, I.E some {@link Health} object.
     *
     * @return The {@link Health} object produced by this {@link HealthContributor}.
     */
    Health getHealthSnapshot();

    /**
     * Adds the provided {@link HealthContributor} as a dependency to this contributor.
     * @param contributor The {@link HealthContributor} to add.
     */
    void add(HealthContributor contributor);

    /**
     * Removes the provided {@link HealthContributor} from its {@link Collection} of child {@link HealthContributor}.
     * @param contributor The {@link HealthContributor} to remove.
     *
     * @return True if the HealthContributor was successfully removed from the contributors collection.
     */
    HealthContributor remove(HealthContributor contributor);

    /**
     * A human-readable identifier used for logging.
     * @return The name provided.
     */
    String getName();
}
