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

import io.pravega.shared.health.impl.HealthComponent;

/**
 * The {@link HealthConfig} interface is the basis for being able to define a 'Health Hierarchy/Tree', that will represent
 * the logical groupings of {@link HealthContributor} objects.
 */
public interface HealthConfig {

    /**
     * A flag used to determine if the {@link HealthConfig} object is logically 'empty'.
     *
     * @return Whether or not the {@link HealthConfig} is empty.
     */
    boolean isEmpty();

    /**
     * A reconcile method makes it clear that a {@link HealthConfig} should provide a way for the {@link HealthComponent}
     * objects defined to be properly registered by the {@link ContributorRegistry}.
     *
     * @param registry The {@link ContributorRegistry} to apply this reconciliation on.
     */
    void reconcile(ContributorRegistry registry);
}
