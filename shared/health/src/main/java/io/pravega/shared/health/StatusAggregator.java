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
 * Defines the interface used to reduce a {@link Collection} of {@link Status} objects produced by some health check
 * into a singular {@link Status}.
 */
@FunctionalInterface
public interface StatusAggregator {

    /**
     * Reduces a {@link Collection} of {@link Status} objects into a single {@link Status} object
     * representing the overall health of the given {@link io.pravega.shared.health.impl.CompositeHealthContributor}.
     *
     * @param statuses The {@link Collection} of {@link Status}.
     * @return The reduced {@link Status}.
     */
    Status aggregate(Collection<Status> statuses);
}