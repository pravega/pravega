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
package io.pravega.common.cluster;


import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Cluster interface enables to register / de-register a Host to a cluster.
 */
public interface Cluster extends AutoCloseable {

    /**
     * Register a Host to a cluster.
     *
     * @param host Host to be part of cluster.
     */
    void registerHost(final Host host);

    /**
     * De-register a Host from a cluster.
     *
     * @param host Host to be removed from cluster.
     */
    void deregisterHost(final Host host);

    /**
     * Add Listeners.
     *
     * @param listener Cluster event listener.
     */
    void addListener(final ClusterListener listener);

    /**
     * Add Listeners with an executor to run the listener on.
     *
     * @param listener Cluster event listener.
     * @param executor Executor to run listener on.
     */
    void addListener(final ClusterListener listener, final Executor executor);

    /**
     * Get the current cluster members.
     *
     * @return List of cluster members.
     */
    Set<Host> getClusterMembers();

    /**
     * Get the health status.
     *
     * @return true/false.
     */
    boolean isHealthy();
}
