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

/**
 * A {@link HealthContributor} is an interface that is able to provide or *contribute* health information relating to
 * an arbitrary component, process, object, etc.
 */
public interface HealthContributor extends AutoCloseable {

    /**
     * The delimiter used to split a fully qualified {@link HealthContributor} name during state look-ups.
     */
    String DELIMITER = "/";

    /**
     * From an abstract view, a {@link HealthContributor} is anything that has an impact on the health of the system.
     * As such it should provide a window into it's current state, I.E some {@link Health} object.
     *
     * @return The {@link Health} object produced by this {@link HealthContributor}.
     */
    Health getHealthSnapshot();

    /**
     * Registers any number of {@link HealthContributor} as children to referenced {@link HealthContributor}.
     * @param children The parent {@link HealthContributor}.
     */
    void register(HealthContributor... children);

    /**
     * A human-readable identifier used for logging.
     * @return The name provided.
     */
    String getName();

    /**
     * Closes the {@link HealthContributor} and forwards the closure to all its children.
     */
    @Override
    void close();

    /**
     * Checks if the {@link HealthContributor} is closed.
     * @return The close result.
     */
    boolean isClosed();
}
