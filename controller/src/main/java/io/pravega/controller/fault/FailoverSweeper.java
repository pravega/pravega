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
package io.pravega.controller.fault;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Interface to be implemented by components that are registered with Cluster Listener to listen for failed host notifications.
 * The components need to implement two methods, once which is invoked during bootstrap, where cluster listener supplies
 * a lambda for fetching all existing processes. Using this, the component needs to determine if there are failed processes that
 * it needs to sweep and takeover their dangling work.
 * Apart from this, the sweeper is also notified whenever a controller process fails. The sweeper can use this notification to
 * attempt to take over sweeper specific dangling work from the failed host. 
 */
public interface FailoverSweeper {
    /**
     * Check if the sweeper is ready to sweep.
     *
     * @return true if ready, false otherwise.
     */
    boolean isReady();

    /**
     * Method to start sweeping all failed processes at the time of bootstrap. It will be called during component start,
     * and the implementer is given a supplier for list of processes. It can then compare its list of processes with the
     * supplied processes to determine failed processes and handle their fail over recovery.
     *
     * @param processes supplier to get all current processes registered and running in the cluster.
     * @return Do not block, process asynchronously and return a future
     */
    CompletableFuture<Void> sweepFailedProcesses(Supplier<Set<String>> processes);

    /**
     * Method to receive notification for failed process.
     *
     * @param failedProcess identifier for the failed process.
     * @return Do not block, process asynchronously and return a future
     */
    CompletableFuture<Void> handleFailedProcess(String failedProcess);
}
