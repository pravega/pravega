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
 * This HealthConnector interface provides a single method definition to help define how some class may attach/register
 * its child fields under the parent HealthContributor (i.e. make it reachable from the {@link HealthServiceManager}).
 *
 * The benefit of isolating this one method is to prevent having to change the constructors of all intermediate classes
 * to include some {@link HealthContributor} object. Instead, a class that does not directly instantiate its own
 * {@link HealthContributor} can forward the register call to the appropriate child objects.
 */
public interface HealthConnector {

    /**
     * Provides the {@link HealthContributor} to be used when connecting the {@link HealthContributor} of some class
     * to a parent {@link HealthContributor}.
     *
     * @return The {@link HealthContributor} belonging to the class implementing this interface.
     */
    default HealthContributor getContributor() {
        return null;
    }

    /**
     * Registers any number of {@link HealthContributor} as children to referenced {@link HealthContributor}.
     *
     * @param parent The parent {@link HealthContributor}.
     */
    default void connect(HealthContributor parent) {
        HealthContributor contributor = getContributor();
        if (contributor != null) {
            parent.register(contributor);
        }
    }

    /**
     * This call should be used as a pass-through method, where the class implementing this method does not have any
     * {@link HealthContributor} objects itself to register, but it has some child objects which may contain some
     * {@link HealthContributor} objects.
     *
     * @param parent The parent {@link HealthConnector}.
     */
    default void connect(HealthConnector parent) {
        // This method is intentionally left blank.
    }
}
